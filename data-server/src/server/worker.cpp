#include "worker.h"

#include <assert.h>
#include <chrono>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"

#include "callback.h"
#include "run_status.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

Worker::Worker(size_t fast_worker_size, size_t slow_worker_size) :
    fast_worker_size_(fast_worker_size),
    slow_worker_size_(slow_worker_size) {
}

void Worker::StartWorker(std::vector<std::thread> &worker,
                         HashQueue &hash_queue, int num) {
    hash_queue.msg_queue.resize(num);

    for (int i = 0; i < num; i++) {
        auto mq = new_lk_queue();
        hash_queue.msg_queue[i] = mq;

        worker.emplace_back([&, mq] {
            common::ProtoMessage *task = nullptr;
            while (g_continue_flag) {
                task = (common::ProtoMessage*)lk_queue_pop(mq); //block mode
                if (task != nullptr) {
                    if (!g_continue_flag) {
                        delete task;
                        break;
                    }
                    --hash_queue.all_msg_size;
                    DealTask(task);
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            }
            FLOG_INFO("Worker thread exit...");
        });
    }
}

int Worker::Start() {
    FLOG_INFO("Worker Start begin ...");

    // start fast worker
    StartWorker(fast_workers_, fast_queue_, ds_config.fast_worker_num);

    int i = 0;
    char fast_name[32] = {'\0'};
    for (auto &work : fast_workers_) {
        auto handle = work.native_handle();
        snprintf(fast_name, 32, "fast_worker:%d", i++);
        AnnotateThread(handle, fast_name);
    }
    // start slow worker
    StartWorker(slow_workers_, slow_queue_, ds_config.slow_worker_num);

    char slow_name[32] = {'\0'};
    i = 0;
    for (auto &work : slow_workers_) {
        auto handle = work.native_handle();
        snprintf(slow_name, 32, "slow_worker:%d", i++);
        AnnotateThread(handle, slow_name);
    }

    FLOG_INFO("Worker Start end ...");
    return 0;
}

void Worker::Stop() {
    FLOG_INFO("Worker Stop begin ...");

    auto size = fast_workers_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (fast_workers_[i].joinable()) {
            fast_workers_[i].join();
        }
    }

    size = slow_workers_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (slow_workers_[i].joinable()) {
            slow_workers_[i].join();
        }
    }

    Clean(fast_queue_);
    Clean(slow_queue_);

    FLOG_INFO("Worker Stop end ...");
}

void Worker::Push(common::ProtoMessage *task) {
    if (isSlow(task)) {
        auto slot = ++slot_seed_ % ds_config.slow_worker_num;
        auto mq = slow_queue_.msg_queue[slot];
        lk_queue_push(mq, task);
        ++slow_queue_.all_msg_size;
    } else {
        auto slot = ++slot_seed_ % ds_config.fast_worker_num;
        auto mq = fast_queue_.msg_queue[slot];
        lk_queue_push(mq, task);
        ++fast_queue_.all_msg_size;
    }
}

void Worker::DealTask(common::ProtoMessage *task) {
    if (task->expire_time < getticks()) {
        FLOG_ERROR("msg_id %" PRIu64 " is expired ", task->header.msg_id);
        delete task;
        return;
    }

    DataServer::Instance().DealTask(task);
}

void Worker::Clean(HashQueue &hash_queue) {
    for (auto mq : hash_queue.msg_queue) {
        while (true) {
            auto task = (common::ProtoMessage *)lk_queue_pop(mq);
            if (task == nullptr) {
                break;
            }
            delete task;
        }
        delete_lk_queue(mq);
    }
}

size_t Worker::ClearQueue(bool fast, bool slow) {
    size_t count = 0;
    if (fast) {
        for (auto& q : fast_queue_.msg_queue) {
            while (true) {
                auto task = (common::ProtoMessage *)lk_queue_pop(q);
                if (task == nullptr) {
                    break;
                }
                delete task;
                ++count;
            }
        }
    }
    if (slow) {
        for (auto& q : slow_queue_.msg_queue) {
            while (true) {
                auto task = (common::ProtoMessage *)lk_queue_pop(q);
                if (task == nullptr) {
                    break;
                }
                delete task;
                ++count;
            }
        }
    }
    return count;
}

bool Worker::isSlow(sharkstore::dataserver::common::ProtoMessage *msg) {
    if ((msg->header.flags & FAST_WORKER_FLAG) != 0) {
        return false;
    }
    switch (msg->header.func_id) {
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
    FLOG_INFO("worker fast queue size:%" PRIu64,
              fast_queue_.all_msg_size.load());
    FLOG_INFO("worker slow queue size:%" PRIu64,
              slow_queue_.all_msg_size.load());
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
