#include "worker_pool.h"

#include "worker.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

WorkerPool::WorkerPool(const std::string& name, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        auto w = new Worker(this, name);
        workers_.push_back(w);
        free_list_.push_back(w);
    }
}

WorkerPool::~WorkPool() {
    for (auto w : workers_) {
        delete w;
    }
}

bool WorkerPool::Post(const std::shared_ptr<SnapTask>& task) {
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

int Runnings() const {
    std::lock_guard<std::mutex> lock(mu_);

    return workers_.size() - free_list_.size();
}

void WorkerPool::addToFreeList(Worker* w) {
    std::lock_guard<std::mutex> lock(mu_);

    free_list_.push_back(w);
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
