cmake_minimum_required(VERSION 3.16)
project(cdc_consumers CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(cpr REQUIRED)

add_executable(consumers
    src/main.cpp
    src/logger.cpp
    src/avro_deserializer.cpp
    src/kafka_consumer_base.cpp
    src/es_indexer.cpp
    src/redis_updater.cpp
    src/notifier.cpp
)

target_include_directories(consumers PRIVATE src /usr/local/include /usr/include)

target_link_libraries(consumers PRIVATE
    Threads::Threads
    cpr::cpr
    rdkafka++ rdkafka
    serdes
    avro
    jansson
    curl
    hiredis
    spdlog fmt
)
