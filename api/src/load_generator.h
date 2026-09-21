#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <httplib.h>

namespace cdc {

class LoadGenerator {
public:
    // num_threads: how many parallel HTTP workers to run.
    // Each worker fires (req_per_sec / num_threads) requests per second independently.
    LoadGenerator(const std::string& api_host, int api_port,
                  int req_per_sec, int num_threads = 10);
    ~LoadGenerator();

    void Start();
    void Stop();

private:
    std::string api_host_;
    int api_port_;
    int req_per_sec_;
    int num_threads_;
    std::atomic<bool> running_;
    std::vector<std::thread> workers_;

    // Each thread runs this independently with its own thread_id (0-indexed)
    void RunWorker(int thread_id);
};

} // namespace cdc
