#pragma once

#include <memory>
#include <chrono>
#include <httplib.h>
#include "database_client.h"
#include "redis_client.h"

namespace cdc {

class ApiServer {
public:
    ApiServer(std::shared_ptr<DatabaseClient> db, std::shared_ptr<RedisClient> redis);
    void Start(int port);

private:
    std::shared_ptr<DatabaseClient> db_;
    std::shared_ptr<RedisClient> redis_;
    httplib::Server svr_;

    void SetupRoutes();
};

} // namespace cdc
