#include "io_context_pool.h"

#include <iostream>

namespace fbase {
namespace dataserver {
namespace net {

IOContextPool::IOContextPool(size_t size) : pool_size_(size) {
    for (size_t i = 0; i < size; ++i) {
        auto context = std::make_shared<asio::io_context>();
        auto guard = asio::make_work_guard(*context);
        io_contexts_.push_back(std::move(context));
        work_guards_.push_back(std::move(guard));
    }
}

IOContextPool::~IOContextPool() {
    // stop if running
    Stop();
}

void IOContextPool::Start() {
    int i = 0;
    for (const auto& ctx : io_contexts_) {
        std::thread t(std::bind(&IOContextPool::runLoop, this, ctx, i++));
        threads_.push_back(std::move(t));
    }
}

void IOContextPool::Stop() {
    if (stopped_) return;

    stopped_ = true;
    work_guards_.clear();
    for (auto& ctx : io_contexts_) {
        ctx->stop();
    }
    for (auto& t : threads_) {
        t.join();
    }
}

asio::io_context& IOContextPool::GetIOContext() {
    auto idx = round_robin_counter_++ % io_contexts_.size();
    return *(io_contexts_[idx]);
}

void IOContextPool::runLoop(const std::shared_ptr<asio::io_context>& ctx,
                            int i) {
    std::cout << "[Net] context pool loop-" << i << " start." << std::endl;

    try {
        ctx->run();
    } catch (std::exception& e) {
        // TODO: log
        std::cerr << "[Net] run context pool loop-" << i
                  << " error: " << e.what() << std::endl;
    }

    std::cout << "[Net] context pool loop-" << i << " exit." << std::endl;
}

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
