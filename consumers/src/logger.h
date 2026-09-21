#pragma once

#include <string>

namespace cdc {

class Logger {
public:
    static void Init(const std::string& logstash_host, int logstash_port, const std::string& service_name);
    static void Shutdown();
};

} // namespace cdc
