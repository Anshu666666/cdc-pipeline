#include "database_client.h"
#include <spdlog/spdlog.h>
#include "uuid_util.h"
#include <stdexcept>
#include <fmt/core.h>

namespace cdc {

DatabaseClient::DatabaseClient(const std::string& connection_string) : conn_str_(connection_string) {
    Connect();
}

DatabaseClient::~DatabaseClient() {
    if (conn_ && conn_->is_open()) {
        conn_->disconnect();
    }
}

void DatabaseClient::Connect() {
    try {
        conn_ = std::make_unique<pqxx::connection>(conn_str_);
        if (!conn_->is_open()) {
            throw std::runtime_error("Can't open database");
        }
        spdlog::info(R"({{"event": "db_connected", "dbname": "{}"}})", conn_->dbname());
    } catch (const std::exception& e) {
        spdlog::critical(R"({{"event": "db_connection_failed", "error": "{}"}})", e.what());
        throw; // Fail fast on startup if DB is down
    }
}

std::string DatabaseClient::CreateOrder(const std::string& user_id, double total_amount, const nlohmann::json& items) {
    std::string order_id = generate_uuid();

    std::lock_guard<std::mutex> lock(db_mutex_);
    try {
        pqxx::work W(*conn_);
        std::string items_str = items.dump();
        
        std::string sql = fmt::format(
            "INSERT INTO orders (id, user_id, total_amount, status, items_jsonb) VALUES ('{}', '{}', {}, 'CREATED', '{}')",
            order_id, user_id, total_amount, items_str
        );
        
        W.exec(sql);
        W.commit();
        
        spdlog::info(R"({{"event": "order_created_db", "order_id": "{}"}})", order_id);
        return order_id;
    } catch (const std::exception& e) {
        spdlog::error(R"({{"event": "db_insert_failed", "error": "{}"}})", e.what());
        // We throw so the API returns 500. No silent failures.
        throw;
    }
}

bool DatabaseClient::UpdateOrderStatus(const std::string& order_id, const std::string& new_status) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    try {
        pqxx::work W(*conn_);
        // Use parameterized query to prevent SQL injection
        pqxx::result R = W.exec_params(
            "UPDATE orders SET status = $1 WHERE id = $2",
            new_status, order_id
        );
        W.commit();

        bool found = (R.affected_rows() > 0);
        if (found) {
            spdlog::info(R"({{"event": "order_status_updated_db", "order_id": "{}", "new_status": "{}"}})",
                         order_id, new_status);
        }
        return found;
    } catch (const std::exception& e) {
        spdlog::error(R"({{"event": "db_update_failed", "order_id": "{}", "error": "{}"}})",
                      order_id, e.what());
        throw;
    }
}

std::optional<nlohmann::json> DatabaseClient::GetOrderById(const std::string& order_id) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    try {
        pqxx::nontransaction N(*conn_);
        std::string sql = fmt::format("SELECT id, user_id, total_amount, status, items_jsonb FROM orders WHERE id = '{}'", order_id);
        pqxx::result R = N.exec(sql);
        
        if (R.empty()) {
            return std::nullopt;
        }
        
        nlohmann::json order_json;
        order_json["id"] = R[0][0].as<std::string>();
        order_json["user_id"] = R[0][1].as<std::string>();
        order_json["total_amount"] = R[0][2].as<double>();
        order_json["status"] = R[0][3].as<std::string>();
        order_json["items_jsonb"] = nlohmann::json::parse(R[0][4].as<std::string>());
        
        return order_json;
    } catch (const std::exception& e) {
        spdlog::error(R"({{"event": "db_read_failed", "error": "{}"}})", e.what());
        throw; // API handles this explicitly
    }
}

} // namespace cdc
