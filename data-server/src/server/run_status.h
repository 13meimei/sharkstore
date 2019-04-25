_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <set>

#include "monitor/system_status.h"
#include "monitor/statistics.h"
#include "range/stats.h"

#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

struct DBUsage {
    uint64_t total_size = 0;
    uint64_t used_size = 0;
    uint64_t free_size = 0;
};

class RunStatus : public range::RangeStats {
public:
    RunStatus();
    ~RunStatus() = default;

    RunStatus(const RunStatus &) = delete;
    RunStatus &operator=(const RunStatus &) = delete;

    int Init(ContextServer *context, const uint64_t seq = 0);
    int Start();
    void Stop();

    void PushTime(monitor::HistogramType type, int64_t time) override {
        if (time > 0) statistics_.PushTime(type, static_cast<uint64_t>(time));
    }

    bool GetDBUsage(DBUsage* usage);
    uint64_t GetDBUsedPercent() const { return db_usage_percent_.load();}

    void ReportLeader(uint64_t range_id, bool is_leader) override;
    uint64_t GetLeaderCount() const;

    void IncrSplitCount() override { ++split_count_; }
    void DecrSplitCount() override { --split_count_; }
    uint64_t GetSplitCount() const { return split_count_; }

private:
    void run();
    void collectDBUsage();
    void printStatistics();
    void printDBMetric();

private:
    ContextServer *context_ = nullptr;

    std::unique_ptr<monitor::SystemStatus> system_status_;
    monitor::Statistics statistics_;

    std::atomic<uint64_t> db_usage_percent_ = {0};
    std::atomic<uint64_t> split_count_ = {0};

    std::set<uint64_t> leaders_;
    mutable std::mutex leaders_mu_;

    uint64_t seq_{0};
    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread metric_thread_;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
