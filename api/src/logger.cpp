#include "logger.h"
#include <spdlog/sinks/tcp_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/pattern_formatter.h>
#include <iostream>

namespace cdc {

void Logger::Init(const std::string& logstash_host, int logstash_port, const std::string& service_name) {
    try {
        // We will log to both console (for local debugging) and TCP (for Logstash)
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        
        // Logstash expects JSON. We can format it manually in the pattern or use a custom formatter, 
        // but for simplicity we construct a JSON-like string via spdlog pattern:
        // {"@timestamp":"%Y-%m-%dT%H:%M:%S.%f%z", "level":"%l", "service":"%n", "message":"%v"}
        auto tcp_sink = std::make_shared<spdlog::sinks::tcp_sink_mt>(
            spdlog::sinks::tcp_sink_config(logstash_host, logstash_port)
        );
        
        std::string json_pattern = "{\"@timestamp\":\"%Y-%m-%dT%H:%M:%S.%f%z\", \"level\":\"%l\", \"service\":\"%n\", \"message\":%v}\n";
        tcp_sink->set_pattern(json_pattern);
        
        std::vector<spdlog::sink_ptr> sinks {console_sink, tcp_sink};
        auto logger = std::make_shared<spdlog::logger>(service_name, sinks.begin(), sinks.end());
        
        // Register globally
        spdlog::set_default_logger(logger);
        spdlog::set_level(spdlog::level::info);
        spdlog::flush_on(spdlog::level::info);
        
        spdlog::info("\"Logger initialized successfully\"");
    } catch (const spdlog::spdlog_ex& ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
        // Strict coding standard: Fail fast if we can't initialize critical observability infrastructure
        std::exit(1);
    }
}

void Logger::Shutdown() {
    spdlog::shutdown();
}

} // namespace cdc
