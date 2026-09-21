#include "logger.h"
#include "avro_deserializer.h"
#include "es_indexer.h"
#include "redis_updater.h"
#include "notifier.h"

#include <spdlog/spdlog.h>
#include <iostream>
#include <thread>
#include <vector>
#include <csignal>
#include <atomic>
#include <chrono>

static std::atomic<bool> g_shutdown_requested{false};

void SignalHandler(int signum) {
    (void)signum;
    g_shutdown_requested = true;
}

std::string GetEnv(const char* key, const std::string& default_val) {
    const char* val = std::getenv(key);
    return val ? std::string(val) : default_val;
}

int main() {
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    std::string kafka_brokers = GetEnv("KAFKA_BROKERS", "kafka:29092");
    std::string schema_registry_url = GetEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    std::string kafka_topic = GetEnv("KAFKA_TOPIC", "dbserver1.public.orders");
    std::string es_url = GetEnv("ES_URL", "http://elasticsearch:9200");
    std::string redis_host = GetEnv("REDIS_HOST", "redis");
    int redis_port = std::stoi(GetEnv("REDIS_PORT", "6379"));
    std::string logstash_host = GetEnv("LOGSTASH_HOST", "logstash");
    int logstash_port = std::stoi(GetEnv("LOGSTASH_PORT", "5000"));

    // 1. Initialize Logstash & Console logger
    cdc::Logger::Init(logstash_host, logstash_port, "cdc-consumers");
    spdlog::info(R"({{"event": "consumers_starting", "brokers": "{}", "topic": "{}", "schema_registry": "{}"}})",
                 kafka_brokers, kafka_topic, schema_registry_url);

    try {
        std::shared_ptr<cdc::AvroDeserializer> deserializer;
        std::unique_ptr<cdc::ESIndexer> es_indexer;
        std::unique_ptr<cdc::RedisUpdater> redis_updater;
        std::unique_ptr<cdc::Notifier> notifier;

        // Startup readiness loop: wait for Kafka, Schema Registry, and Redis to be ready
        for (int attempt = 1; attempt <= 20; ++attempt) {
            try {
                if (!deserializer) {
                    deserializer = std::make_shared<cdc::AvroDeserializer>(schema_registry_url);
                }
                if (!es_indexer) {
                    es_indexer = std::make_unique<cdc::ESIndexer>(kafka_brokers, kafka_topic, es_url, deserializer);
                }
                if (!redis_updater) {
                    redis_updater = std::make_unique<cdc::RedisUpdater>(kafka_brokers, kafka_topic, redis_host, redis_port, deserializer);
                }
                if (!notifier) {
                    notifier = std::make_unique<cdc::Notifier>(kafka_brokers, kafka_topic, redis_host, redis_port, deserializer);
                }
                break;
            } catch (const std::exception& e) {
                if (attempt == 20) throw;
                spdlog::warn(R"({{"event": "waiting_for_services", "attempt": {}, "error": "{}"}})", attempt, e.what());
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }

        // Launch 3 background consumer threads
        spdlog::info(R"({{"event": "launching_consumer_threads"}})");
        std::thread es_thread([&es_indexer]() {
            try {
                es_indexer->Run();
            } catch (const std::exception& e) {
                spdlog::critical(R"({{"event": "es_indexer_crashed", "error": "{}"}})", e.what());
            }
        });

        std::thread redis_thread([&redis_updater]() {
            try {
                redis_updater->Run();
            } catch (const std::exception& e) {
                spdlog::critical(R"({{"event": "redis_updater_crashed", "error": "{}"}})", e.what());
            }
        });

        std::thread notifier_thread([&notifier]() {
            try {
                notifier->Run();
            } catch (const std::exception& e) {
                spdlog::critical(R"({{"event": "notifier_crashed", "error": "{}"}})", e.what());
            }
        });

        spdlog::info(R"({{"event": "all_consumer_threads_running"}})");

        // Main thread waits for shutdown signal
        while (!g_shutdown_requested) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        spdlog::info(R"({{"event": "shutdown_signal_received"}})");

        if (es_indexer) es_indexer->Stop();
        if (redis_updater) redis_updater->Stop();
        if (notifier) notifier->Stop();

        if (es_thread.joinable()) es_thread.join();
        if (redis_thread.joinable()) redis_thread.join();
        if (notifier_thread.joinable()) notifier_thread.join();

        spdlog::info(R"({{"event": "consumers_shutdown_complete"}})");
    } catch (const std::exception& e) {
        spdlog::critical(R"({{"event": "consumers_fatal_error", "error": "{}"}})", e.what());
        cdc::Logger::Shutdown();
        return 1;
    }

    cdc::Logger::Shutdown();
    return 0;
}
