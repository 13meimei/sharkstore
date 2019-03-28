#include "common_thread.h"
namespace sharkstore {
namespace dataserver {

void Work::Do() {
    if (!(*stopped)) {
        f0();
    }
}

WorkThread::WorkThread(size_t queue_capcity, const std::string& name)
    : capacity_(queue_capcity), running_(true) {
    assert(capacity_ > 0);

    thr_.reset(new std::thread(std::bind(&WorkThread::run, this)));
    // 设置线程名称
    AnnotateThread(thr_->native_handle(), name.c_str());
}

WorkThread::~WorkThread() { shutdown(); }

bool WorkThread::submit(uint64_t owner, std::atomic<bool>* stopped,
                        const std::function<void()>& f0,
                        std::string& cmd) {

    bool notify = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;

        if (queue_.size() >= capacity_) {
            return false;
        } else {
            Work w;
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

bool WorkThread::tryPost(const Work& w) {
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

void WorkThread::post(const Work& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return;
        queue_.push(w);
    }
    cv_.notify_one();
}

void WorkThread::waitPost(const Work& w) {
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

void WorkThread::shutdown() {
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_one();
    thr_->join();
}

bool WorkThread::pull(Work* w) {
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

void WorkThread::run() {
    while (true) {
        Work work;
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

int WorkThread::size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

}
}
