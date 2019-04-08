#include "common_thread.h"

namespace sharkstore {
namespace dataserver {
namespace server {

void CommonWork::Do() {
    if (!(*stopped)) {
        f0();
    }
}

CommonThread::CommonThread(size_t queue_capcity, const std::string& name)
    : capacity_(queue_capcity), running_(true) {
    assert(capacity_ > 0);

    thr_.reset(new std::thread(std::bind(&CommonThread::run, this)));
    // 设置线程名称
    AnnotateThread(thr_->native_handle(), name.c_str());
}

CommonThread::~CommonThread() { shutdown(); }

bool CommonThread::submit(uint64_t owner, std::atomic<bool>* stopped,
                        const std::function<void()>& f0,
                        std::string& cmd) {

    bool notify = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;

        if (queue_.size() >= capacity_) {
            return false;
        } else {
            CommonWork w;
            w.owner = owner;
            w.stopped = stopped;
            w.f0 = f0;
            queue_.push(w);
            notify = true;
        }
    }
    if (notify) {
        cv_.notify_one();
    }
    return true;
}

bool CommonThread::tryPost(const CommonWork& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;
        if (queue_.size() >= capacity_) {
            return false;
        } else {
            queue_.push(w);
        }
    }
    cv_.notify_one();
    return true;
}

void CommonThread::post(const CommonWork& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
        queue_.push(w);
    }
    cv_.notify_one();
}

void CommonThread::waitPost(const CommonWork& w) {
    std::unique_lock<std::mutex> lock(mu_);
    while (queue_.size() >= capacity_ && running_) {
        cv_.wait(lock);
    }

    if (running_) {
        queue_.push(w);
        lock.unlock();
        cv_.notify_one();
    }
}

void CommonThread::shutdown() {
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_one();
    if (thr_->joinable()) {
        thr_->join();
    }
}

bool CommonThread::pull(CommonWork* w) {
    std::unique_lock<std::mutex> lock(mu_);

    while (queue_.empty() && running_) {
        cv_.wait(lock);
    }
    if (!running_) return false;
    *w = queue_.front();
    queue_.pop();

    lock.unlock();
    cv_.notify_one();  // 通知队列已经不再满了
    return true;
}

void CommonThread::run() {
    while (true) {
        CommonWork work;
        if (pull(&work)) {
            try {
                work.Do();
            } catch(...) {
                //FLOG_ERROR("Do error: unknown error occured.");
            }
        } else {
            // shutdown
            return;
        }
    }
}

int CommonThread::size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}


} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */
