#pragma once

#include <string>
#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>
#include <optional>
#include <mutex>

namespace cdc {

class RedisClient {
public:
    RedisClient(const std::string& host, int port);
    ~RedisClient();

    // Returns nullopt if missing, throws if connection fails
    std::optional<std::string> Get(const std::string& key);

private:
    std::string host_;
    int port_;
    redisContext* context_;
    std::mutex redis_mutex_;

    void Connect();
};

} // namespace cdc
