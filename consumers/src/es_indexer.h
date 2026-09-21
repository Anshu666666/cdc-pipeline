#pragma once

#include "kafka_consumer_base.h"
#include <string>

namespace cdc {

class ESIndexer : public KafkaConsumerBase {
public:
    ESIndexer(const std::string& brokers,
              const std::string& topic,
              const std::string& es_url,
              std::shared_ptr<AvroDeserializer> deserializer);

protected:
    void ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) override;

private:
    std::string es_url_;
};

} // namespace cdc
