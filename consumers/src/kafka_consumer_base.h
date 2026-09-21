#pragma once

#include <string>
#include <atomic>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include "avro_deserializer.h"

namespace cdc {

class KafkaConsumerBase {
public:
    KafkaConsumerBase(const std::string& consumer_name,
                      const std::string& brokers,
                      const std::string& group_id,
                      const std::string& topic,
                      std::shared_ptr<AvroDeserializer> deserializer,
                      int max_retries = 5,
                      int initial_backoff_ms = 100,
                      int max_backoff_ms = 3000);
    virtual ~KafkaConsumerBase();

    void Run();
    void Stop();

protected:
    virtual void ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) = 0;
    std::string GetConsumerName() const { return consumer_name_; }

    bool RouteToDLQ(const RdKafka::Message* msg, const std::string& error_reason, const nlohmann::json& payload_json = nullptr);

private:
    void InitProducer();

    std::string consumer_name_;
    std::string brokers_;
    std::string group_id_;
    std::string topic_;
    std::string dlq_topic_;
    std::shared_ptr<AvroDeserializer> deserializer_;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::unique_ptr<RdKafka::Producer> producer_;
    std::atomic<bool> running_{false};

    int max_retries_{5};
    int initial_backoff_ms_{100};
    int max_backoff_ms_{3000};
};

} // namespace cdc
