#ifndef __RUN_STATUS_H__
#define __RUN_STATUS_H__

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>

#include "common/socket_client.h"
#include "frame/sf_status.h"
#include "monitor/isystemstatus.h"
#include "monitor/syscommon.h"
#include "range/statistics.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

struct FileSystemUsage {
    uint64_t total_size = 0;
    uint64_t used_size = 0;
    uint64_t free_size = 0;
};

class RunStatus : public range::RangeStats {
public:
    RunStatus() = default;
    ~RunStatus() = default;

    RunStatus(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();

    void PushTime(monitor::PrintTag type, uint32_t time) override {
        system_status_.PutTopData(type, time);
    }

    bool GetFilesystemUsage(FileSystemUsage* usage);
    uint64_t GetFilesystemUsedPercent() const override { return fs_usage_percent_.load();}

    void IncrLeaderCount() override { ++leader_count_; }
    void DecrLeaderCount() override { --leader_count_; }
    uint64_t GetLeaderCount() const { return leader_count_; }

    void IncrSplitCount() override { ++split_count_; }
    void DecrSplitCount() override { --split_count_; }
    uint64_t GetSplitCount() const { return split_count_; }

private:
    void run();
    void updateFSUsagePercent();
    void printDBMetric();

private:
    ContextServer *context_ = nullptr;

    monitor::ISystemStatus system_status_;
    std::atomic<uint64_t> fs_usage_percent_ = {0};

    std::atomic<uint64_t> leader_count_ = {0};
    std::atomic<uint64_t> split_count_ = {0};

    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread metric_thread_;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
#endif  //__RUN_STATUS_H__
