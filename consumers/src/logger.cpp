#include "logger.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/tcp_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <iostream>
#include <vector>

namespace cdc {

void Logger::Init(const std::string& logstash_host, int logstash_port, const std::string& service_name) {
    try {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        
        auto tcp_sink = std::make_shared<spdlog::sinks::tcp_sink_mt>(
            spdlog::sinks::tcp_sink_config(logstash_host, logstash_port)
        );
        
        std::string json_pattern = "{\"@timestamp\":\"%Y-%m-%dT%H:%M:%S.%f%z\", \"level\":\"%l\", \"service\":\"%n\", \"message\":%v}\n";
        tcp_sink->set_pattern(json_pattern);
        
        std::vector<spdlog::sink_ptr> sinks {console_sink, tcp_sink};
        auto logger = std::make_shared<spdlog::logger>(service_name, sinks.begin(), sinks.end());
        
        spdlog::set_default_logger(logger);
        spdlog::set_level(spdlog::level::info);
        spdlog::flush_on(spdlog::level::info);
        
        spdlog::info(R"({{"event": "logger_initialized", "service": "{}"}})", service_name);
    } catch (const std::exception& ex) {
        std::cerr << "Log initialization warning: " << ex.what() << std::endl;
        // Fallback to console logger only if TCP sink is temporarily unreachable
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto logger = std::make_shared<spdlog::logger>(service_name, console_sink);
        spdlog::set_default_logger(logger);
        spdlog::set_level(spdlog::level::info);
        spdlog::warn(R"({{"event": "logger_tcp_failed_using_console_only", "error": "{}"}})", ex.what());
    }
}

void Logger::Shutdown() {
    spdlog::shutdown();
}

} // namespace cdc
