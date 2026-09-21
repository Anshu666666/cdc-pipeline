#pragma once

#include <string>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>
#include <memory>
#include <mutex>
#include <optional>

namespace cdc {

class DatabaseClient {
public:
    DatabaseClient(const std::string& connection_string);
    ~DatabaseClient();

    // Throws on fatal error (e.g. DB connection lost)
    std::string CreateOrder(const std::string& user_id, double total_amount, const nlohmann::json& items);

    // Returns true if the row was found and updated, false if not found
    bool UpdateOrderStatus(const std::string& order_id, const std::string& new_status);

    // Explicit return types for graceful degradation
    std::optional<nlohmann::json> GetOrderById(const std::string& order_id);

private:
    std::string conn_str_;
    std::unique_ptr<pqxx::connection> conn_;
    std::mutex db_mutex_;
    
    void Connect();
};

} // namespace cdc
