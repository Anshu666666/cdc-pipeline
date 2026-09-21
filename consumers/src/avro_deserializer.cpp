#include "avro_deserializer.h"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <cstdint>

extern "C" {
#include <avro.h>
#include <libserdes/serdes-avro.h>
}

namespace cdc {

AvroDeserializer::AvroDeserializer(const std::string& schema_registry_url) {
    char errstr[512] = {0};
    serdes_conf_t* sconf = serdes_conf_new(NULL, 0,
                                           "schema.registry.url", schema_registry_url.c_str(),
                                           NULL);
    if (!sconf) {
        throw std::runtime_error("Failed to create serdes config");
    }
    
    serdes_ = serdes_new(sconf, errstr, sizeof(errstr));
    if (!serdes_) {
        throw std::runtime_error(std::string("Failed to initialize libserdes: ") + errstr);
    }
    spdlog::info(R"({{"event": "avro_deserializer_initialized", "schema_registry": "{}"}})", schema_registry_url);
}

AvroDeserializer::~AvroDeserializer() {
    if (serdes_) {
        serdes_destroy(serdes_);
    }
}

std::optional<nlohmann::json> AvroDeserializer::Deserialize(const void* payload, size_t len) {
    if (!payload || len == 0) {
        return std::nullopt; // Tombstone message
    }

    const uint8_t* bytes = static_cast<const uint8_t*>(payload);

    // Confluent Schema Registry binary encoding: Magic byte is 0x00, followed by 4 bytes schema ID
    if (len >= 5 && bytes[0] == 0x00) {
        char errstr[512] = {0};
        serdes_schema_t* schema = nullptr;
        avro_value_t avro_val;

        int r = serdes_deserialize_avro(serdes_, &avro_val, &schema,
                                        payload, len,
                                        errstr, sizeof(errstr));
        if (r != 0) {
            spdlog::error(R"({{"event": "avro_deserialization_failed", "error": "{}"}})", errstr);
            return std::nullopt;
        }

        char* json_str = nullptr;
        if (avro_value_to_json(&avro_val, 1, &json_str) != 0 || !json_str) {
            avro_value_decref(&avro_val);
            spdlog::error(R"({{"event": "avro_to_json_conversion_failed"}})");
            return std::nullopt;
        }

        nlohmann::json doc;
        try {
            doc = nlohmann::json::parse(json_str);
        } catch (const std::exception& e) {
            spdlog::error(R"({{"event": "json_parse_failed", "error": "{}"}})", e.what());
        }

        free(json_str);
        avro_value_decref(&avro_val);
        return doc;
    }

    // Fallback if message is plain JSON
    try {
        std::string raw_str(static_cast<const char*>(payload), len);
        return nlohmann::json::parse(raw_str);
    } catch (const std::exception& e) {
        spdlog::error(R"({{"event": "raw_payload_parse_failed", "error": "{}"}})", e.what());
        return std::nullopt;
    }
}

} // namespace cdc
