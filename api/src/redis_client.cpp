#include "redis_client.h"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <fmt/core.h>

namespace cdc {

RedisClient::RedisClient(const std::string& host, int port) : host_(host), port_(port), context_(nullptr) {
    Connect();
}

RedisClient::~RedisClient() {
    if (context_) {
        redisFree(context_);
    }
}

void RedisClient::Connect() {
    context_ = redisConnect(host_.c_str(), port_);
    if (context_ == nullptr || context_->err) {
        if (context_) {
            std::string err_str = context_->errstr;
            redisFree(context_);
            context_ = nullptr;
            throw std::runtime_error(fmt::format("Redis connection error: {}", err_str));
        } else {
            throw std::runtime_error("Redis connection error: Can't allocate redis context");
        }
    }
    
    // Set a timeout for fast failure (100ms)
    struct timeval tv = {0, 100000}; 
    redisSetTimeout(context_, tv);
    
    spdlog::info(R"({{"event": "redis_connected", "host": "{}"}})", host_);
}

std::optional<std::string> RedisClient::Get(const std::string& key) {
    std::lock_guard<std::mutex> lock(redis_mutex_);
    
    if (!context_) {
        // We throw instead of swallowing the error. The API layer catches this and falls back to Postgres.
        throw std::runtime_error("Redis context is null");
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "GET %s", key.c_str());
    if (reply == nullptr) {
        // Connection died or timeout
        redisFree(context_);
        context_ = nullptr;
        throw std::runtime_error("Redis command failed or timed out");
    }

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return std::nullopt; // Cache miss
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string result = reply->str;
        freeReplyObject(reply);
        return result;
    }

    freeReplyObject(reply);
    throw std::runtime_error("Unexpected Redis reply type");
}

} // namespace cdc
