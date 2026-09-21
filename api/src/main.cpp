#include "api_server.h"
#include "database_client.h"
#include "redis_client.h"
#include "load_generator.h"
#include "logger.h"

#include <iostream>
#include <memory>
#include <csignal>
#include <thread>
#include <chrono>
#include <spdlog/spdlog.h>

std::unique_ptr<cdc::LoadGenerator> load_gen = nullptr;

void signal_handler(int signal) {
    spdlog::info(R"({{"event": "shutdown_signal_received", "signal": {}}})", signal);
    if (load_gen) {
        load_gen->Stop();
    }
    cdc::Logger::Shutdown();
    std::exit(signal);
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Get environment variables or defaults
    std::string db_conn = std::getenv("DB_CONN") ? std::getenv("DB_CONN") : "postgresql://ecomm:password@postgres:5432/ecomm";
    std::string redis_host = std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "redis";
    int redis_port = std::getenv("REDIS_PORT") ? std::stoi(std::getenv("REDIS_PORT")) : 6379;
    std::string logstash_host = std::getenv("LOGSTASH_HOST") ? std::getenv("LOGSTASH_HOST") : "logstash";
    int logstash_port = std::getenv("LOGSTASH_PORT") ? std::stoi(std::getenv("LOGSTASH_PORT")) : 5000;
    
    // Initialize JSON logging to Logstash
    cdc::Logger::Init(logstash_host, logstash_port, "order-api");

    try {
        std::shared_ptr<cdc::DatabaseClient> db;
        std::shared_ptr<cdc::RedisClient> redis;

        // Startup readiness loop: wait for Postgres and Redis to be ready
        for (int attempt = 1; attempt <= 20; ++attempt) {
            try {
                if (!db) db = std::make_shared<cdc::DatabaseClient>(db_conn);
                if (!redis) redis = std::make_shared<cdc::RedisClient>(redis_host, redis_port);
                break;
            } catch (const std::exception& e) {
                if (attempt == 20) throw;
                spdlog::warn(R"({{"event": "waiting_for_db_or_redis", "attempt": {}, "error": "{}"}})", attempt, e.what());
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }

        // Setup API Server
        cdc::ApiServer server(db, redis);
        
        // Load generator rate controlled by env vars — tunable without recompile
        int load_rps     = std::getenv("LOAD_GEN_RPS")     ? std::stoi(std::getenv("LOAD_GEN_RPS"))     : 100;
        int load_threads = std::getenv("LOAD_GEN_THREADS") ? std::stoi(std::getenv("LOAD_GEN_THREADS")) : 10;
        spdlog::info(R"({{"event": "load_generator_config", "rps": {}, "threads": {}}})", load_rps, load_threads);

        load_gen = std::make_unique<cdc::LoadGenerator>("localhost", 8080, load_rps, load_threads);
        load_gen->Start();

        // Start listening (blocking call)
        server.Start(8080);
        
    } catch (const std::exception& e) {
        spdlog::critical(R"({{"event": "fatal_startup_error", "error": "{}"}})", e.what());
        cdc::Logger::Shutdown();
        return 1;
    }

    return 0;
}
