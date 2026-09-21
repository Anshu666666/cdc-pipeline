#include "load_generator.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <random>
#include <fmt/core.h>
#include "uuid_util.h"

namespace cdc {

static const std::vector<std::string> kStatusCycle = {
    "PROCESSING", "SHIPPED", "DELIVERED"
};

LoadGenerator::LoadGenerator(const std::string& api_host, int api_port,
                             int req_per_sec, int num_threads)
    : api_host_(api_host), api_port_(api_port),
      req_per_sec_(req_per_sec), num_threads_(num_threads),
      running_(false) {}

LoadGenerator::~LoadGenerator() {
    Stop();
}

void LoadGenerator::Start() {
    running_ = true;
    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
        workers_.emplace_back(&LoadGenerator::RunWorker, this, i);
    }
    spdlog::info(
        R"({{"event": "load_generator_started", "target": "{}:{}", "req_per_sec": {}, "threads": {}}})",
        api_host_, api_port_, req_per_sec_, num_threads_);
}

void LoadGenerator::Stop() {
    running_ = false;
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
    workers_.clear();
    spdlog::info(R"({{"event": "load_generator_stopped"}})");
}

void LoadGenerator::RunWorker(int thread_id) {
    httplib::Client cli(api_host_, api_port_);
    cli.set_connection_timeout(1, 0);   // 1s connection timeout
    cli.set_read_timeout(5, 0);         // 5s read timeout

    // Each thread targets its share of the total RPS
    int thread_rps = std::max(1, req_per_sec_ / num_threads_);
    auto sleep_duration = std::chrono::microseconds(1'000'000 / thread_rps);

    std::mt19937 gen(std::random_device{}() ^ static_cast<uint32_t>(thread_id));
    std::uniform_real_distribution<> amount_dist(9.99, 999.99);
    // Used to occasionally send PATCH requests on already-created orders
    std::uniform_int_distribution<> patch_chance(0, 4); // 1-in-5 chance
    std::uniform_int_distribution<> status_pick(0, static_cast<int>(kStatusCycle.size()) - 1);

    // Small queue of recently created order IDs for PATCH requests
    std::vector<std::string> created_ids;
    created_ids.reserve(64);

    while (running_) {
        auto tick_start = std::chrono::steady_clock::now();

        bool do_patch = !created_ids.empty() && (patch_chance(gen) == 0);

        if (do_patch) {
            // ── PATCH /orders/:id/status ──────────────────────────────
            std::string order_id = created_ids[gen() % created_ids.size()];
            std::string new_status = kStatusCycle[status_pick(gen)];

            nlohmann::json body = {{"status", new_status}};
            std::string path = "/api/v1/orders/" + order_id + "/status";

            auto t0 = std::chrono::steady_clock::now();
            auto res = cli.Patch(path.c_str(), body.dump(), "application/json");
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();

            int status_code = res ? res->status : 0;
            if (res && (res->status == 200 || res->status == 204)) {
                spdlog::info(
                    R"({{"event": "request_sent", "method": "PATCH", "thread_id": {}, "order_id": "{}", "new_status": "{}", "status_code": {}, "latency_ms": {}}})",
                    thread_id, order_id, new_status, status_code, latency_ms);
            } else {
                spdlog::warn(
                    R"({{"event": "request_failed", "method": "PATCH", "thread_id": {}, "status_code": {}, "latency_ms": {}}})",
                    thread_id, status_code, latency_ms);
            }
        } else {
            // ── POST /api/v1/orders ───────────────────────────────────
            nlohmann::json items = nlohmann::json::array();
            items.push_back({{"product_id", "prod_" + std::to_string((gen() % 20) + 1)},
                              {"qty", static_cast<int>((gen() % 5) + 1)}});

            nlohmann::json payload = {
                {"user_id",      generate_uuid()},
                {"total_amount", amount_dist(gen)},
                {"items",        items}
            };

            auto t0 = std::chrono::steady_clock::now();
            auto res = cli.Post("/api/v1/orders", payload.dump(), "application/json");
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();

            int status_code = res ? res->status : 0;
            if (res && res->status == 201) {
                // Capture the new order_id for future PATCH requests
                try {
                    auto resp_json = nlohmann::json::parse(res->body);
                    if (resp_json.contains("id") && resp_json["id"].is_string()) {
                        std::string new_id = resp_json["id"].get<std::string>();
                        if (created_ids.size() >= 512) {
                            // Ring-buffer: evict oldest to bound memory
                            created_ids.erase(created_ids.begin());
                        }
                        created_ids.push_back(new_id);
                    }
                } catch (...) {}

                spdlog::info(
                    R"({{"event": "request_sent", "method": "POST", "thread_id": {}, "status_code": {}, "latency_ms": {}}})",
                    thread_id, status_code, latency_ms);
            } else {
                spdlog::warn(
                    R"({{"event": "request_failed", "method": "POST", "thread_id": {}, "status_code": {}, "latency_ms": {}}})",
                    thread_id, status_code, latency_ms);
            }
        }

        // Pace this thread to its target RPS
        auto elapsed = std::chrono::steady_clock::now() - tick_start;
        if (elapsed < sleep_duration) {
            std::this_thread::sleep_for(sleep_duration - elapsed);
        }
    }
}

} // namespace cdc
