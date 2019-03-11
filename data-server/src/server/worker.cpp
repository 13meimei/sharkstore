#include "worker.h"

#include <assert.h>
#include <chrono>
#include <stdio.h>

#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"
#include "proto/gen/funcpb.pb.h"

#include "run_status.h"
#include "range_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

static const size_t kWorkThreadCapacity = 100000;

Worker::WorkThread::WorkThread(WorkThreadGroup* group, const std::string& name, size_t max_capacity) :
    parent_group_(group),
    capacity_(max_capacity) {
    thr_ = std::thread([this] { this->runLoop(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
}

Worker::WorkThread::~WorkThread() {
    if (thr_.joinable()) {
        thr_.join();
    }
}

bool Worker::WorkThread::Push(RPCRequest* msg) {
    que_.push(msg);
    return true;
}

uint64_t Worker::WorkThread::PendingSize() const {
    return que_.unsafe_size();
}

uint64_t Worker::WorkThread::Clear() {
    uint64_t count = 0;
    RPCRequest* task = nullptr;
    while (g_continue_flag) {
        if (que_.try_pop(task)) {
            delete task;
            ++count;
        } else {
            break;
        }
    }
    return count;
}

void Worker::WorkThread::runLoop() {
    RPCRequest *task = nullptr;
    while (g_continue_flag) {
        if (que_.try_pop(task)) {
            parent_group_->DealTask(task);
        } else {
            if (!g_continue_flag) return;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}


Worker::WorkThreadGroup::WorkThreadGroup(RangeServer* rs, int num,
        size_t capacity_per_thread, const std::string& name) :
    rs_(rs),
    thread_num_(num),
    capacity_(capacity_per_thread),
    name_(name) {
}

Worker::WorkThreadGroup::~WorkThreadGroup() {
    for (const auto& thr: threads_) {
        delete thr;
    }
    threads_.clear();
}

void Worker::WorkThreadGroup::Start() {
    for (int i = 0; i < thread_num_; ++i) {
        char buf[16] = {'\0'};
        snprintf(buf, 16, "%s:%d", name_.c_str(), i);
        auto thr = new WorkThread(this, buf, capacity_);
        threads_.push_back(thr);
    }
}

bool Worker::WorkThreadGroup::Push(RPCRequest* msg) {
    auto idx = ++round_robin_counter_ % threads_.size();
    return threads_[idx]->Push(msg);
}

void Worker::WorkThreadGroup::DealTask(RPCRequest* req_ptr) {
    // transfer request ownership to range server
    std::unique_ptr<RPCRequest> req(req_ptr);
    rs_->DealTask(std::move(req));
}

uint64_t Worker::WorkThreadGroup::PendingSize() const {
    uint64_t size = 0;
    for (auto thr: threads_) {
        size += thr->PendingSize();
    }
    return size;
}

uint64_t Worker::WorkThreadGroup::Clear() {
    uint64_t size = 0;
    for (auto thr: threads_) {
        size += thr->Clear();
    }
    return size;
}


Status Worker::Start(int fast_worker_size, int slow_worker_size, RangeServer* range_server) {
    FLOG_INFO("Worker Start begin, fast_worker_size: %d, slow_worker_size: %d",
            fast_worker_size, slow_worker_size);

    range_server_ = range_server;

    fast_workers_.reset(
            new WorkThreadGroup(range_server, fast_worker_size, kWorkThreadCapacity, "fast_worker"));
    fast_workers_->Start();
    slow_workers_.reset(
            new WorkThreadGroup(range_server, fast_worker_size, kWorkThreadCapacity, "slow_worker"));
    slow_workers_->Start();

    FLOG_INFO("Worker Start end ...");

    return Status::OK();
}

void Worker::Stop() {
}

void Worker::Push(RPCRequest* task) {
    if (!isSlowTask(task)) {
        fast_workers_->Push(task);
    } else {
        slow_workers_->Push(task);
    }
}

void Worker::Deal(RPCRequest* req) {
    std::unique_ptr<RPCRequest> req_ptr(req);
    range_server_->DealTask(std::move(req_ptr));
}

size_t Worker::ClearQueue(bool fast, bool slow) {
    size_t count = 0;
    if (fast) {
        count += fast_workers_->Clear();
    }
    if (slow) {
        count += slow_workers_->Clear();
    }
    return count;
}

bool Worker::isSlowTask(RPCRequest *task) {
    const auto& head = task->msg->head;
    if (head.ForceFastFlag()) {
        return false;
    }
    switch (head.func_id) {
        case funcpb::FunctionID::kFuncSelect:
        case funcpb::FunctionID::kFuncUpdate:
        case funcpb::FunctionID::kFuncWatchGet:
        case funcpb::FunctionID::kFuncKvRangeDel:
        case funcpb::FunctionID::kFuncKvScan :
            return true;
        default:
            return false;
    }
}

void Worker::PrintQueueSize() {
    FLOG_INFO("worker fast queue size:%" PRIu64, fast_workers_->PendingSize());
    FLOG_INFO("worker slow queue size:%" PRIu64, slow_workers_->PendingSize());
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
