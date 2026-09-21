cmake_minimum_required(VERSION 3.14)
project(cdc_api_server CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(api_server
    src/main.cpp
    src/api_server.cpp
    src/database_client.cpp
    src/redis_client.cpp
    src/load_generator.cpp
    src/logger.cpp
)

target_include_directories(api_server PRIVATE src)

# Link against system libraries installed via apt-get
target_link_libraries(api_server PRIVATE
    pqxx pq
    hiredis
    spdlog fmt
)

find_package(Threads REQUIRED)
target_link_libraries(api_server PRIVATE Threads::Threads)
