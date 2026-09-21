#include "redis_updater.h"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <chrono>

namespace cdc {

RedisUpdater::RedisUpdater(const std::string& brokers,
                           const std::string& topic,
                           const std::string& redis_host,
                           int redis_port,
                           std::shared_ptr<AvroDeserializer> deserializer)
    : KafkaConsumerBase("RedisUpdater", brokers, "redis-updater-group", topic, deserializer),
      redis_host_(redis_host), redis_port_(redis_port) {
    EnsureConnected();
}

RedisUpdater::~RedisUpdater() {
    std::lock_guard<std::mutex> lock(redis_mtx_);
    if (redis_ctx_) {
        redisFree(redis_ctx_);
        redis_ctx_ = nullptr;
    }
}

void RedisUpdater::EnsureConnected() {
    std::lock_guard<std::mutex> lock(redis_mtx_);
    if (!redis_ctx_ || redis_ctx_->err) {
        if (redis_ctx_) {
            redisFree(redis_ctx_);
            redis_ctx_ = nullptr;
        }
        redis_ctx_ = redisConnect(redis_host_.c_str(), redis_port_);
        if (!redis_ctx_ || redis_ctx_->err) {
            std::string err = redis_ctx_ ? redis_ctx_->errstr : "Allocation failure";
            throw std::runtime_error("RedisUpdater failed to connect to Redis: " + err);
        }
        struct timeval tv = {1, 0};
        redisSetTimeout(redis_ctx_, tv);
        spdlog::info(R"({{"event": "redis_updater_connected", "host": "{}"}})", redis_host_);
    }
}

int64_t RedisUpdater::ExtractLSN(const nlohmann::json& record) {
    if (record.contains("source") && record["source"].is_object()) {
        const auto& src = record["source"];
        if (src.contains("lsn") && !src["lsn"].is_null()) {
            if (src["lsn"].is_number()) {
                return src["lsn"].get<int64_t>();
            } else if (src["lsn"].is_string()) {
                try { return std::stoll(src["lsn"].get<std::string>()); } catch (...) {}
            } else if (src["lsn"].is_object()) {
                for (auto& [k, v] : src["lsn"].items()) {
                    if (v.is_number()) return v.get<int64_t>();
                    if (v.is_string()) {
                        try { return std::stoll(v.get<std::string>()); } catch (...) {}
                    }
                }
            }
        }
        if (src.contains("ts_ms") && src["ts_ms"].is_number()) {
            return src["ts_ms"].get<int64_t>();
        }
    }
    if (record.contains("ts_ms") && record["ts_ms"].is_number()) {
        return record["ts_ms"].get<int64_t>();
    }
    return 0;
}

void RedisUpdater::ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) {
    nlohmann::json order_data;
    if (record.contains("after") && !record["after"].is_null()) {
        order_data = record["after"];
    } else if (record.contains("order_id") || record.contains("id")) {
        order_data = record;
    } else {
        // Handle delete event
        if (record.contains("op") && record["op"] == "d" && record.contains("before") && !record["before"].is_null()) {
            auto before_data = record["before"];
            if (before_data.is_object()) {
                if (before_data.contains("Value") && before_data["Value"].is_object()) {
                    before_data = before_data["Value"];
                } else if (before_data.size() == 1 && before_data.begin().value().is_object() && !before_data.contains("id")) {
                    before_data = before_data.begin().value();
                }
            }
            std::string order_id = "";
            if (before_data.contains("id") && !before_data["id"].is_null()) {
                order_id = before_data["id"].is_string() ? before_data["id"].get<std::string>() : before_data["id"].dump();
            } else if (before_data.contains("order_id") && !before_data["order_id"].is_null()) {
                order_id = before_data["order_id"].is_string() ? before_data["order_id"].get<std::string>() : before_data["order_id"].dump();
            }
            if (order_id.empty()) {
                order_id = key;
            }
            if (!order_id.empty()) {
                EnsureConnected();
                std::lock_guard<std::mutex> lock(redis_mtx_);
                std::string redis_key = "order:" + order_id;
                int64_t lsn = ExtractLSN(record);
                std::string lsn_str = std::to_string(lsn);

                const char* lua_del =
                    "local current_lsn = redis.call('GET', KEYS[1] .. ':lsn'); "
                    "if current_lsn and tonumber(ARGV[1]) and tonumber(current_lsn) and tonumber(ARGV[1]) <= tonumber(current_lsn) then "
                    "    return 0 "
                    "end; "
                    "redis.call('DEL', KEYS[1]); "
                    "redis.call('SET', KEYS[1] .. ':lsn', ARGV[1], 'EX', ARGV[2]); "
                    "return 1;";

                redisReply* reply = (redisReply*)redisCommand(
                    redis_ctx_, "EVAL %s 1 %b %s %s",
                    lua_del,
                    redis_key.data(), (size_t)redis_key.size(),
                    lsn_str.c_str(),
                    "86400"
                );
                if (!reply || reply->type == REDIS_REPLY_ERROR) {
                    std::string err = reply ? reply->str : "Null reply / connection error";
                    if (reply) freeReplyObject(reply);
                    throw std::runtime_error("Redis delete failed: " + err);
                }
                freeReplyObject(reply);
                spdlog::info(R"({{"event": "redis_deleted", "order_id": "{}", "lsn": {}, "offset": {}}})", order_id, lsn, offset);
            }
        }
        return;
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
        spdlog::warn(R"({{"event": "redis_updater_skip_no_id", "offset": {}}})", offset);
        return;
    }

    int64_t lsn = ExtractLSN(record);
    std::string lsn_str = std::to_string(lsn);

    EnsureConnected();
    std::lock_guard<std::mutex> lock(redis_mtx_);
    std::string redis_key = "order:" + order_id;
    std::string order_json_str = order_data.dump();

    // Atomic LSN-checked upsert via Lua:
    // If incoming LSN <= current LSN in Redis, discard as stale replay to prevent data loss / corruption
    const char* lua_upsert =
        "local current_lsn = redis.call('GET', KEYS[1] .. ':lsn'); "
        "if current_lsn and tonumber(ARGV[2]) and tonumber(current_lsn) and tonumber(ARGV[2]) <= tonumber(current_lsn) then "
        "    return 0 "
        "end; "
        "redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3]); "
        "redis.call('SET', KEYS[1] .. ':lsn', ARGV[2], 'EX', ARGV[3]); "
        "return 1;";

    auto redis_t0 = std::chrono::steady_clock::now();
    redisReply* reply = (redisReply*)redisCommand(
        redis_ctx_, "EVAL %s 1 %b %b %s %s",
        lua_upsert,
        redis_key.data(), (size_t)redis_key.size(),
        order_json_str.data(), (size_t)order_json_str.size(),
        lsn_str.c_str(),
        "86400"
    );
    auto sink_latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - redis_t0).count();

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        std::string err = reply ? reply->str : "Null reply / connection error";
        if (reply) freeReplyObject(reply);
        throw std::runtime_error("Redis upsert failed: " + err);
    }

    if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
        freeReplyObject(reply);
        spdlog::warn(R"({{"event": "redis_stale_event_ignored", "order_id": "{}", "lsn": {}, "offset": {}}})",
                     order_id, lsn, offset);
        return;
    }

    freeReplyObject(reply);

    std::string status = order_data.value("status", "UNKNOWN");
    spdlog::info(
        R"({{"event": "redis_updated", "consumer": "RedisUpdater", "order_id": "{}", "status": "{}", "lsn": {}, "offset": {}, "sink_latency_ms": {}}})",
        order_id, status, lsn, offset, sink_latency_ms);
}

} // namespace cdc
