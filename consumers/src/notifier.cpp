#include "notifier.h"
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace cdc {

Notifier::Notifier(const std::string& brokers,
                   const std::string& topic,
                   const std::string& redis_host,
                   int redis_port,
                   std::shared_ptr<AvroDeserializer> deserializer)
    : KafkaConsumerBase("Notifier", brokers, "notifier-group", topic, deserializer),
      redis_host_(redis_host), redis_port_(redis_port) {
    EnsureConnected();
}

Notifier::~Notifier() {
    std::lock_guard<std::mutex> lock(redis_mtx_);
    if (redis_ctx_) {
        redisFree(redis_ctx_);
        redis_ctx_ = nullptr;
    }
}

void Notifier::EnsureConnected() {
    std::lock_guard<std::mutex> lock(redis_mtx_);
    if (!redis_ctx_ || redis_ctx_->err) {
        if (redis_ctx_) {
            redisFree(redis_ctx_);
            redis_ctx_ = nullptr;
        }
        redis_ctx_ = redisConnect(redis_host_.c_str(), redis_port_);
        if (!redis_ctx_ || redis_ctx_->err) {
            std::string err = redis_ctx_ ? redis_ctx_->errstr : "Allocation failure";
            throw std::runtime_error("Notifier failed to connect to Redis: " + err);
        }
        struct timeval tv = {1, 0};
        redisSetTimeout(redis_ctx_, tv);
        spdlog::info(R"({{"event": "notifier_redis_connected", "host": "{}"}})", redis_host_);
    }
}

void Notifier::ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) {
    nlohmann::json order_data;
    if (record.contains("after") && !record["after"].is_null()) {
        order_data = record["after"];
    } else if (record.contains("order_id") || record.contains("id")) {
        order_data = record;
    } else {
        return; // Ignore delete/other non-order events
    }

    if (order_data.is_object()) {
        if (order_data.contains("Value") && order_data["Value"].is_object()) {
            order_data = order_data["Value"];
        } else if (order_data.size() == 1 && order_data.begin().value().is_object() && !order_data.contains("id")) {
            order_data = order_data.begin().value();
        }
    }

    std::string order_id = "";
    if (order_data.contains("id") && !order_data["id"].is_null()) {
        order_id = order_data["id"].is_string() ? order_data["id"].get<std::string>() : order_data["id"].dump();
    } else if (order_data.contains("order_id") && !order_data["order_id"].is_null()) {
        order_id = order_data["order_id"].is_string() ? order_data["order_id"].get<std::string>() : order_data["order_id"].dump();
    }
    if (order_id.empty()) {
        order_id = key;
    }
    if (order_id.empty()) {
        return;
    }

    std::string status = order_data.value("status", "");
    std::string customer_id = "guest";
    if (order_data.contains("user_id") && !order_data["user_id"].is_null()) {
        customer_id = order_data["user_id"].is_string() ? order_data["user_id"].get<std::string>() : order_data["user_id"].dump();
    } else if (order_data.contains("customer_id") && !order_data["customer_id"].is_null()) {
        customer_id = order_data["customer_id"].is_string() ? order_data["customer_id"].get<std::string>() : order_data["customer_id"].dump();
    }

    // We send notification when status changes to SHIPPED or DELIVERED
    if (status == "SHIPPED" || status == "DELIVERED") {
        EnsureConnected();
        std::lock_guard<std::mutex> lock(redis_mtx_);
        
        // Idempotency: SET with NX and 24h expiration (binary-safe)
        // Key format: email_sent:<order_id>:<status>
        std::string idempotency_key = "email_sent:" + order_id + ":" + status;
        
        redisReply* reply = (redisReply*)redisCommand(
            redis_ctx_, "SET %b 1 EX 86400 NX",
            idempotency_key.data(), (size_t)idempotency_key.size()
        );
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            std::string err = reply ? reply->str : "Null reply / connection error";
            if (reply) freeReplyObject(reply);
            throw std::runtime_error("Notifier Redis command failed: " + err);
        }

        // If Redis returns "OK", the key was newly set (first time processing)
        if (reply->type == REDIS_REPLY_STATUS && std::string(reply->str) == "OK") {
            freeReplyObject(reply);
            
            // Simulated Email dispatch
            spdlog::info(R"({{"event": "email_notification_sent", "consumer": "Notifier", "recipient": "{}@example.com", "order_id": "{}", "status": "{}", "subject": "Order {} has been {}", "offset": {}}})",
                         customer_id, order_id, status, order_id, status, offset);
        } else {
            freeReplyObject(reply);
            
            // Duplicate detected and safely skipped
            spdlog::warn(R"({{"event": "email_notification_skipped_duplicate", "consumer": "Notifier", "order_id": "{}", "status": "{}", "reason": "Idempotency key already exists in Redis", "offset": {}}})",
                         order_id, status, offset);
        }
    }
}

} // namespace cdc
