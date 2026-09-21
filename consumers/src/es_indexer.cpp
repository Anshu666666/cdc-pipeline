#include "es_indexer.h"
#include <spdlog/spdlog.h>
#include <cpr/cpr.h>

namespace cdc {

ESIndexer::ESIndexer(const std::string& brokers,
                     const std::string& topic,
                     const std::string& es_url,
                     std::shared_ptr<AvroDeserializer> deserializer)
    : KafkaConsumerBase("ESIndexer", brokers, "es-indexer-group", topic, deserializer), es_url_(es_url) {
}

void ESIndexer::ProcessRecord(const nlohmann::json& record, const std::string& key, int64_t offset) {
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
                std::string delete_url = es_url_ + "/orders/_doc/" + order_id;
                cpr::Response r = cpr::Delete(cpr::Url{delete_url}, cpr::Timeout{5000});
                if (r.status_code >= 200 && r.status_code < 300) {
                    spdlog::info(R"({{"event": "es_deleted", "order_id": "{}", "status": {}}})", order_id, r.status_code);
                } else if (r.status_code != 404) {
                    throw std::runtime_error("Elasticsearch delete failed: status " + std::to_string(r.status_code) + ", error: " + r.error.message);
                }
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
        spdlog::warn(R"({{"event": "es_indexer_skip_no_id", "offset": {}}})", offset);
        return;
    }

    std::string doc_url = es_url_ + "/orders/_doc/" + order_id;
    auto es_t0 = std::chrono::steady_clock::now();
    cpr::Response r = cpr::Put(
        cpr::Url{doc_url},
        cpr::Header{{"Content-Type", "application/json"}},
        cpr::Body{order_data.dump()},
        cpr::Timeout{5000}
    );
    auto sink_latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - es_t0).count();

    if (r.status_code >= 200 && r.status_code < 300) {
        spdlog::info(
            R"({{"event": "es_indexed", "consumer": "ESIndexer", "order_id": "{}", "status_code": {}, "offset": {}, "sink_latency_ms": {}}})",
            order_id, r.status_code, offset, sink_latency_ms);
    } else {
        // Throw exception to trigger KafkaConsumerBase exponential backoff and preserve offset
        throw std::runtime_error("Elasticsearch indexing failed: status " + std::to_string(r.status_code) +
                                 ", error: " + r.error.message + ", response: " + r.text);
    }
}

} // namespace cdc
