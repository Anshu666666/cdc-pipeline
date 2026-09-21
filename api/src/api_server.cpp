#include "api_server.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <fmt/core.h>

namespace cdc {

ApiServer::ApiServer(std::shared_ptr<DatabaseClient> db, std::shared_ptr<RedisClient> redis) 
    : db_(db), redis_(redis) {
    SetupRoutes();
}

void ApiServer::SetupRoutes() {
    // ── POST /api/v1/orders ──────────────────────────────────────────
    svr_.Post("/api/v1/orders", [this](const httplib::Request& req, httplib::Response& res) {
        auto t0 = std::chrono::steady_clock::now();
        try {
            auto j = nlohmann::json::parse(req.body);
            std::string user_id = j.at("user_id").get<std::string>();
            double total_amount = j.at("total_amount").get<double>();
            auto items = j.at("items");

            std::string order_id = db_->CreateOrder(user_id, total_amount, items);
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();

            spdlog::info(
                R"({{"event": "api_order_created", "order_id": "{}", "latency_ms": {}}})",
                order_id, latency_ms);

            nlohmann::json response = {
                {"id", order_id},
                {"status", "CREATED"}
            };
            
            res.status = 201;
            res.set_content(response.dump(), "application/json");
        } catch (const nlohmann::json::exception& e) {
            spdlog::warn(R"({{"event": "api_bad_request", "error": "{}"}})", e.what());
            res.status = 400;
            res.set_content(R"({"error": "Invalid JSON payload"})", "application/json");
        } catch (const std::exception& e) {
            spdlog::error(R"({{"event": "api_server_error", "error": "{}"}})", e.what());
            res.status = 500;
            res.set_content(R"({"error": "Internal Server Error"})", "application/json");
        }
    });

    // ── PATCH /api/v1/orders/:id/status ─────────────────────────────
    svr_.Patch(R"(/api/v1/orders/([^/]+)/status)", [this](const httplib::Request& req, httplib::Response& res) {
        auto t0 = std::chrono::steady_clock::now();
        std::string order_id = req.matches[1];
        try {
            auto j = nlohmann::json::parse(req.body);
            std::string new_status = j.at("status").get<std::string>();

            bool updated = db_->UpdateOrderStatus(order_id, new_status);
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();

            if (updated) {
                spdlog::info(
                    R"({{"event": "api_order_status_updated", "order_id": "{}", "new_status": "{}", "latency_ms": {}}})",
                    order_id, new_status, latency_ms);
                res.status = 200;
                res.set_content(R"({"updated": true})", "application/json");
            } else {
                res.status = 404;
                res.set_content(R"({"error": "Order not found"})", "application/json");
            }
        } catch (const nlohmann::json::exception& e) {
            spdlog::warn(R"({{"event": "api_bad_request", "error": "{}"}})", e.what());
            res.status = 400;
            res.set_content(R"({"error": "Invalid JSON payload"})", "application/json");
        } catch (const std::exception& e) {
            spdlog::error(R"({{"event": "api_server_error", "error": "{}"}})", e.what());
            res.status = 500;
            res.set_content(R"({"error": "Internal Server Error"})", "application/json");
        }
    });

    svr_.Get(R"(/api/v1/orders/([^/]+))", [this](const httplib::Request& req, httplib::Response& res) {
        std::string order_id = req.matches[1];
        
        try {
            // 1. Attempt Redis Cache Read
            try {
                auto cache_result = redis_->Get(fmt::format("order:{}", order_id));
                if (cache_result) {
                    spdlog::info(R"({{"event": "api_cache_hit", "order_id": "{}"}})", order_id);
                    nlohmann::json response = nlohmann::json::parse(*cache_result);
                    response["source"] = "redis";
                    res.status = 200;
                    res.set_content(response.dump(), "application/json");
                    return;
                } else {
                    spdlog::info(R"({{"event": "api_cache_miss", "order_id": "{}"}})", order_id);
                }
            } catch (const std::exception& e) {
                // ENGINEERED FALLBACK: Log explicit warning and degrade to Postgres
                spdlog::warn(R"({{"event": "api_redis_fallback", "error": "{}", "order_id": "{}"}})", e.what(), order_id);
            }

            // 2. Fallback to Postgres
            auto db_result = db_->GetOrderById(order_id);
            if (db_result) {
                spdlog::info(R"({{"event": "api_db_hit", "order_id": "{}"}})", order_id);
                nlohmann::json response = *db_result;
                response["source"] = "postgres";
                res.status = 200;
                res.set_content(response.dump(), "application/json");
            } else {
                res.status = 404;
                res.set_content(R"({"error": "Order not found"})", "application/json");
            }

        } catch (const std::exception& e) {
            spdlog::error(R"({{"event": "api_server_error", "error": "{}"}})", e.what());
            res.status = 500;
            res.set_content(R"({"error": "Internal Server Error"})", "application/json");
        }
    });
}

void ApiServer::Start(int port) {
    spdlog::info(R"({{"event": "api_server_starting", "port": {}}})", port);
    svr_.listen("0.0.0.0", port);
}

} // namespace cdc
