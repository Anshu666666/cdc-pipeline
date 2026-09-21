#pragma once

#include "kafka_consumer_base.h"
#include <string>
#include <hiredis/hiredis.h>
#include <mutex>

namespace cdc {

class RedisUpdater : public KafkaConsumerBase {
public:
    RedisUpdater(const std::string& brokers,
                 const std::string& topic,
                 const std::string& redis_host,
                 int redis_port,
                 std::shared_ptr<AvroDeserializer> deserializer);
    ~RedisUpdater() override;

protected:
    void ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) override;

private:
    void EnsureConnected();
    int64_t ExtractLSN(const nlohmann::json& record);

    std::string redis_host_;
    int redis_port_;
    redisContext* redis_ctx_{nullptr};
    std::mutex redis_mtx_;
};

} // namespace cdc
