#pragma once

#include <string>
#include <spdlog/spdlog.h>

namespace cdc {

class Logger {
public:
    // Initialize the logger to send JSON logs to Logstash via TCP
    static void Init(const std::string& logstash_host, int logstash_port, const std::string& service_name);
    
    // Explicit shutdown to flush logs
    static void Shutdown();
};

} // namespace cdc
