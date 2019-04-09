_Pragma("once");

#include <atomic>
#include <functional>
#include <mutex>
#include <condition_variable>

#include "meta_file.h"
#include "storage.h"
#include "log_file.h"

namespace sharkstore {

namespace test { class StorageTest; }

namespace raft {
namespace impl {
namespace storage {

class LogFile;
using PointerLogFile = std::shared_ptr<LogFile>;

class VecLogFile : public std::vector<PointerLogFile> {

public:
    VecLogFile() = default;
    ~VecLogFile() = default; 

    VecLogFile(const VecLogFile&) = delete;
    VecLogFile& operator=(const VecLogFile&) = delete;

    void push_back(PointerLogFile& in) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<PointerLogFile>::push_back(in);
    }

    void push_back(PointerLogFile&& in) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<PointerLogFile>::push_back(in);
    }

    std::vector<PointerLogFile>::iterator erase(std::vector<PointerLogFile>::iterator it) {
        std::lock_guard<std::mutex> lock(mtx_);
        return std::vector<PointerLogFile>::erase(it);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<PointerLogFile>::clear();
    }

    Status GetCommitFiles(const uint64_t apply_index, std::vector<PointerLogFile>& vec) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto &i : *this) {
                if (i->GetFullFlag() == 0) break;
                if (apply_index > i->LastIndex()) continue;
                vec.emplace_back(i);
            }
        }
        if (vec.empty()) {
            return Status(Status::kNotFound, "Not found raft log file", "");
        }
        return Status::OK();
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx_);
        return std::vector<PointerLogFile>::empty();
    }

    void pop_back() {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<PointerLogFile>::pop_back();
    }

private:
    std::condition_variable cond_;
    std::mutex mtx_;
};


class DiskStorage : public Storage {
#ifndef NDEBUG
public:
    friend class sharkstore::test::StorageTest;
#endif

public:
    struct Options {
        // 一个日志文件的大小
        size_t log_file_size = 1024 * 1024 * 16;

        // 最多保留多少个日志文件，超过此数则截断旧文件
        size_t max_log_files = std::numeric_limits<size_t>::max();

        bool allow_corrupt_startup = false;
        // 创建时日志的起始index，之前的视作被截断
        uint64_t initial_first_index = 0;

        // 每次操作都执行sync
        bool always_sync = false;

        // 只读模式打开
        bool readonly = false;
    };

    DiskStorage(uint64_t id, const std::string& path, const Options& ops,
                const std::function<Status(uint64_t&)>& f0);
    ~DiskStorage();

    DiskStorage(const DiskStorage&) = delete;
    DiskStorage& operator=(const DiskStorage&) = delete;

    Status Open() override;

    Status StoreHardState(const pb::HardState& hs) override;
    Status InitialState(pb::HardState* hs) const override;

    Status StoreEntries(const std::vector<EntryPtr>& entries) override;
    Status Term(uint64_t index, uint64_t* term, bool* is_compacted) const override;
    Status FirstIndex(uint64_t* index) const override;
    Status LastIndex(uint64_t* index) const override;
    Status Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                   std::vector<EntryPtr>* entries, bool* is_compacted) const override;

    Status Truncate(uint64_t index) override;

    Status ApplySnapshot(const pb::SnapshotMeta& meta) override;

    void AppliedTo(uint64_t applied) override;
    uint64_t Applied() const { return applied_; }

    Status Close() override;
    Status Destroy(bool backup = false) override;

    size_t FilesCount() const { return log_files_.size(); }
    size_t CommitFileCount() const { return log_files_commited_.size(); }

    std::vector<std::shared_ptr<LogFile>>& GetCommitFiles();

    Status LoadCommitFiles(const uint64_t idx);

// for tests
#ifndef NDEBUG
    void TEST_Add_Corruption1();
    void TEST_Add_Corruption2();
    void TEST_Add_Corruption3();
#endif

private:
    static Status checkLogsValidate(const std::map<uint64_t, uint64_t>& logs);

    Status initDir();
    Status initMeta();
    Status listLogs(std::map<uint64_t, uint64_t>* logs);
    Status openLogs();
    Status closeLogs();

    // 截断旧日志
    Status truncateOld(uint64_t index);
    // 截断最新的日志
    Status truncateNew(uint64_t index);
    // 清空日志（应用快照时）
    Status truncateAll();

    Status tryRotate();
    Status save(const EntryPtr& e);

private:
    const uint64_t id_ = 0;
    const std::string path_;
    const Options ops_;

    MetaFile meta_file_;
    pb::HardState hard_state_;
    pb::TruncateMeta trunc_meta_;
    uint64_t applied_ = 0;  // 大于applied_的不可截断

    //std::vector<std::shared_ptr<LogFile>> log_files_;
    VecLogFile log_files_;
    VecLogFile log_files_truncated_;
    uint64_t last_index_ = 0;

    std::vector<PointerLogFile> log_files_commited_;
    std::atomic<bool> destroyed_ = {false};
    std::function<Status(uint64_t&)> get_apply_index;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
