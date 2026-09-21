#include "kafka_consumer_base.h"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <vector>
#include <chrono>

namespace cdc {

KafkaConsumerBase::KafkaConsumerBase(const std::string& consumer_name,
                                     const std::string& brokers,
                                     const std::string& group_id,
                                     const std::string& topic,
                                     std::shared_ptr<AvroDeserializer> deserializer,
                                     int max_retries,
                                     int initial_backoff_ms,
                                     int max_backoff_ms)
    : consumer_name_(consumer_name),
      brokers_(brokers),
      group_id_(group_id),
      topic_(topic),
      dlq_topic_(topic + ".dlq"),
      deserializer_(deserializer),
      max_retries_(max_retries),
      initial_backoff_ms_(initial_backoff_ms),
      max_backoff_ms_(max_backoff_ms) {
    
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    if (conf->set("bootstrap.servers", brokers_, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Kafka conf failed (bootstrap.servers): " + errstr);
    }
    if (conf->set("group.id", group_id_, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Kafka conf failed (group.id): " + errstr);
    }
    if (conf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Kafka conf failed (auto.offset.reset): " + errstr);
    }
    // Hardened Zero-Data-Loss: Disable background auto-commit
    if (conf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Kafka conf failed (enable.auto.commit): " + errstr);
    }
    if (conf->set("enable.auto.offset.store", "false", errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Kafka conf failed (enable.auto.offset.store): " + errstr);
    }

    consumer_ = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
        throw std::runtime_error("Failed to create KafkaConsumer: " + errstr);
    }

    std::vector<std::string> topics = {topic_};
    RdKafka::ErrorCode resp = consumer_->subscribe(topics);
    if (resp != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("Failed to subscribe to " + topic_ + ": " + RdKafka::err2str(resp));
    }

    InitProducer();

    spdlog::info(R"({{"event": "consumer_subscribed", "consumer": "{}", "topic": "{}", "group_id": "{}", "dlq_topic": "{}", "max_retries": {}}})",
                 consumer_name_, topic_, group_id_, dlq_topic_, max_retries_);
}

void KafkaConsumerBase::InitProducer() {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> pconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    pconf->set("bootstrap.servers", brokers_, errstr);
    pconf->set("acks", "all", errstr);
    pconf->set("message.timeout.ms", "5000", errstr);
    producer_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(pconf.get(), errstr));
    if (!producer_) {
        spdlog::warn(R"({{"event": "dlq_producer_init_warning", "consumer": "{}", "error": "{}"}})",
                     consumer_name_, errstr);
    }
}

KafkaConsumerBase::~KafkaConsumerBase() {
    Stop();
}

bool KafkaConsumerBase::RouteToDLQ(const RdKafka::Message* msg, const std::string& error_reason, const nlohmann::json& payload_json) {
    if (!producer_) {
        spdlog::error(R"({{"event": "dlq_producer_unavailable", "consumer": "{}"}})", consumer_name_);
        return false;
    }

    nlohmann::json dlq_envelope = {
        {"consumer", consumer_name_},
        {"original_topic", msg->topic_name()},
        {"original_partition", msg->partition()},
        {"original_offset", msg->offset()},
        {"error", error_reason},
        {"timestamp_ms", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()},
        {"payload", payload_json.is_null() ? nlohmann::json(std::string(static_cast<const char*>(msg->payload()), msg->len())) : payload_json}
    };

    std::string key = (msg->key() && msg->key_len() > 0) ? std::string(msg->key()->data(), msg->key_len()) : "";
    std::string dlq_body = dlq_envelope.dump();

    RdKafka::ErrorCode resp = producer_->produce(
        dlq_topic_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(dlq_body.data()), dlq_body.size(),
        key.empty() ? nullptr : key.data(), key.size(),
        0, nullptr
    );

    if (resp != RdKafka::ERR_NO_ERROR) {
        spdlog::error(R"({{"event": "dlq_produce_failed", "consumer": "{}", "error": "{}"}})",
                      consumer_name_, RdKafka::err2str(resp));
        return false;
    }

    producer_->poll(0);
    producer_->flush(1000);
    spdlog::warn(R"({{"event": "routed_to_dlq", "consumer": "{}", "dlq_topic": "{}", "offset": {}, "error": "{}"}})",
                 consumer_name_, dlq_topic_, msg->offset(), error_reason);
    return true;
}

void KafkaConsumerBase::Run() {
    running_ = true;
    while (running_) {
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
        if (!msg) {
            continue;
        }

        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR: {
                std::string key = "";
                if (msg->key() && msg->key_len() > 0) {
                    const uint8_t* kbytes = reinterpret_cast<const uint8_t*>(msg->key()->data());
                    if (msg->key_len() >= 5 && kbytes[0] == 0x00) {
                        auto key_doc = deserializer_->Deserialize(msg->key()->data(), msg->key_len());
                        if (key_doc.has_value()) {
                            if (key_doc->contains("id") && !(*key_doc)["id"].is_null()) {
                                key = (*key_doc)["id"].is_string() ? (*key_doc)["id"].get<std::string>() : (*key_doc)["id"].dump();
                            } else if (key_doc->contains("order_id") && !(*key_doc)["order_id"].is_null()) {
                                key = (*key_doc)["order_id"].is_string() ? (*key_doc)["order_id"].get<std::string>() : (*key_doc)["order_id"].dump();
                            } else {
                                key = key_doc->dump();
                            }
                        }
                    }
                    if (key.empty()) {
                        key = std::string(msg->key()->data(), msg->key_len());
                    }
                }

                auto doc_opt = deserializer_->Deserialize(msg->payload(), msg->len());
                if (!doc_opt.has_value()) {
                    // Malformed/unparseable message: route raw payload to DLQ and commit offset
                    spdlog::error(R"({{"event": "unparseable_payload_routing_dlq", "consumer": "{}", "offset": {}}})",
                                  consumer_name_, msg->offset());
                    RouteToDLQ(msg.get(), "Payload deserialization failed");
                    consumer_->commitSync(msg.get());
                    break;
                }

                // Retry loop with exponential backoff
                bool success = false;
                std::string last_error;
                int backoff_ms = initial_backoff_ms_;

                for (int attempt = 1; attempt <= max_retries_ && running_; ++attempt) {
                    try {
                        auto proc_t0 = std::chrono::steady_clock::now();
                        ProcessRecord(*doc_opt, key, msg->offset());
                        auto proc_latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - proc_t0).count();
                        spdlog::info(
                            R"({{"event": "record_processed", "consumer": "{}", "offset": {}, "processing_latency_ms": {}}})",
                            consumer_name_, msg->offset(), proc_latency_ms);
                        success = true;
                        break;
                    } catch (const std::exception& e) {
                        last_error = e.what();
                        spdlog::warn(R"({{"event": "record_processing_failed", "consumer": "{}", "attempt": {}, "max_retries": {}, "offset": {}, "error": "{}"}})",
                                     consumer_name_, attempt, max_retries_, msg->offset(), e.what());
                        if (attempt < max_retries_ && running_) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                            backoff_ms = std::min(backoff_ms * 2, max_backoff_ms_);
                        }
                    }
                }

                if (success) {
                    consumer_->commitSync(msg.get());
                } else if (running_) {
                    // Exhausted retries: Route to DLQ and commit offset so partition is not deadlocked
                    spdlog::error(R"({{"event": "retries_exhausted_routing_dlq", "consumer": "{}", "offset": {}, "error": "{}"}})",
                                  consumer_name_, msg->offset(), last_error);
                    RouteToDLQ(msg.get(), last_error, *doc_opt);
                    consumer_->commitSync(msg.get());
                }
                break;
            }
            case RdKafka::ERR__TIMED_OUT:
            case RdKafka::ERR__PARTITION_EOF:
                break;
            default:
                spdlog::warn(R"({{"event": "kafka_consumer_warning", "consumer": "{}", "error": "{}"}})",
                             consumer_name_, msg->errstr());
                break;
        }
    }

    consumer_->close();
    if (producer_) {
        producer_->flush(2000);
    }
    spdlog::info(R"({{"event": "consumer_stopped", "consumer": "{}"}})", consumer_name_);
}

void KafkaConsumerBase::Stop() {
    running_ = false;
}

} // namespace cdc
