#include "worker_pool.h"

#include "worker.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

WorkerPool::WorkerPool(size_t size) {
    for (size_t i = 0; i < size; ++i) {
        auto w = new Worker(this);
        workers_.push_back(w);
        free_list_.push_back(w);
    }
}

WorkerPool::~WorkPool() {
    for (auto w : workers_) {
        delete w;
    }
}

bool WorkerPool::post(const std::shared_ptr<Task>& task) {
    Worker* w = nullptr;

    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!free_workers.empty()) {
            w = free_list_.front();
            free_list_.pop_front();
        }
    }

    if (w != nullptr) {
        w->post(task);
        return true;
    } else {
        return false;
    }
}

void WorkerPool::addToFreeList(Worker* w) {
    std::unique_lock<std::mutex> lock(mu_);

    free_list_.push_back(w);
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
