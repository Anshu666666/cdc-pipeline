#pragma once

#include "kafka_consumer_base.h"
#include <string>
#include <hiredis/hiredis.h>
#include <mutex>

namespace cdc {

class Notifier : public KafkaConsumerBase {
public:
    Notifier(const std::string& brokers,
             const std::string& topic,
             const std::string& redis_host,
             int redis_port,
             std::shared_ptr<AvroDeserializer> deserializer);
    ~Notifier() override;

protected:
    void ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) override;

private:
    void EnsureConnected();

    std::string redis_host_;
    int redis_port_;
    redisContext* redis_ctx_{nullptr};
    std::mutex redis_mtx_;
};

} // namespace cdc
