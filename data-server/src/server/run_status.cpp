#include "run_status.h"

#include <chrono>
#include <thread>

#include <fastcommon/shared_func.h>
#include <common/ds_config.h>

#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_logger.h"
#include "master/worker.h"

#include "server.h"
#include "worker.h"

namespace sharkstore {
namespace dataserver {
namespace server {

int RunStatus::Init(ContextServer *context, const uint64_t seq) {
    context_ = context;
    seq_ = seq;
    return 0;
}

int RunStatus::Start() {
    FLOG_INFO("RunStatus Start begin ...");

    metric_thread_ = std::thread(&RunStatus::run, this);
    auto handle = metric_thread_.native_handle();
    AnnotateThread(handle, "metric_hb");

    return 0;
}

void RunStatus::Stop() {
    FLOG_INFO("RunStatus Stop begin ...");

    cond_.notify_all();

    if (metric_thread_.joinable()) {
        metric_thread_.join();
    }
}

void RunStatus::run() {
    while (g_continue_flag) {
        collectDiskUsage();
        printDBMetric();
        context_->worker->PrintQueueSize();
        printStatistics();

        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait_for(lock,
                       std::chrono::seconds(ds_config.metric_config.interval));
    }
}

bool RunStatus::GetFilesystemUsage(FileSystemUsage* usage) {
    uint64_t total = 0, available = 0;
    const char *tmp = ds_config.rocksdb_config.path;
    if (1 == seq_) {
        tmp = ds_config.async_rocksdb_config.path;
    }
    if (system_status_.GetFileSystemUsage(tmp, &total, &available)) {
        if (total > 0 && available <= total) {
            usage->total_size = total;
            usage->free_size = available;
            usage->used_size = total - available;
            return true;
        } else {
            FLOG_ERROR("collect filesystem usage error(invalid size: %" PRIu64 ":%" PRIu64 ") ",
                    total, available);
            return false;
        }
    } else {
        FLOG_ERROR("collect filesystem usage error: %s", strErrno(errno).c_str());
        return false;
    }
}

void RunStatus::ReportLeader(uint64_t range_id, bool is_leader) {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    if (is_leader) {
        leaders_.insert(range_id);
    } else {
        leaders_.erase(range_id);
    }
}

uint64_t RunStatus::GetLeaderCount() const {
    std::lock_guard<std::mutex> lock(leaders_mu_);
    return leaders_.size();
}

// 定时采集磁盘使用率
void RunStatus::collectDiskUsage() {
    FileSystemUsage usage;
    if (GetFilesystemUsage(&usage)) {
        fs_usage_percent_ = usage.used_size * 100/ usage.total_size;
    }
}

void RunStatus::printStatistics() {
    FLOG_INFO("\n%s", statistics_.ToString().c_str());
    statistics_.Reset();
}

void RunStatus::printDBMetric() {
    assert(context_->db != nullptr);
    context_->db->PrintMetric();
    if (1 == seq_) {
        context_->pdb->PrintMetric();
    }
}


}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
