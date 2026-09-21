#pragma once

#include <string>
#include <optional>
#include <nlohmann/json.hpp>

extern "C" {
#include <libserdes/serdes-avro.h>
}

namespace cdc {

class AvroDeserializer {
public:
    explicit AvroDeserializer(const std::string& schema_registry_url);
    ~AvroDeserializer();

    // Deserializes binary Avro message with Confluent 5-byte header into nlohmann::json
    std::optional<nlohmann::json> Deserialize(const void* payload, size_t len);

private:
    serdes_t* serdes_{nullptr};
};

} // namespace cdc
