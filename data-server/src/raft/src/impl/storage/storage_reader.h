_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>

#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/db_interface.h"
#include "meta_file.h"
#include "storage_disk.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "../work_thread.h"

namespace sharkstore {
namespace dataserver {
namespace storage {
    static const std::string kPersistRaftLogPrefix = "\x06";
}}}

namespace sharkstore {
namespace raft {
namespace impl {
    class RaftServerImpl;
    class WorkThread;
}}}

namespace sharkstore {
namespace raft {
namespace impl {
namespace storage {

using StorageThread = sharkstore::raft::impl::WorkThread;

class StorageReader : public std::enable_shared_from_this<StorageReader> {
public:
    StorageReader(const uint64_t id,
              std::function<bool(const std::string&)> f0,
              std::function<bool(const metapb::Range &meta)> f1,
              raft::impl::RaftServerImpl *server,
              dataserver::storage::DbInterface* db,
              sharkstore::raft::impl::WorkThread* trd);
    ~ StorageReader();

    StorageReader(const StorageReader&) = delete;
    StorageReader& operator=(const StorageReader&) = delete;

    Status Run();
    size_t GetCommitFiles();
    Status ProcessFiles();

    Status ApplyRaftCmd(const raft_cmdpb::Command& cmd);
    Status StoreAppliedIndex(const uint64_t& seq, const uint64_t& index);
    Status AppliedTo(uint64_t index);
    Status ApplySnapshot(const pb::SnapshotMeta&meta);

    uint64_t Applied() const { return applied_; };

    //index: range applied index
    Status Notify(const uint64_t range_id, const uint64_t index);
    Status Close();

private:
    std::unique_ptr<raft_cmdpb::Command> decodeEntry(EntryPtr entry);
    Status storeRawPut(const raft_cmdpb::Command &cmd);
    Status saveApplyIndex(uint64_t range_id, uint64_t apply_index);

    bool tryPost(const std::function<void()>& f);
    Status listLogs();

    std::function<bool(const std::string& key)> keyInRange;
    std::function<bool(const metapb::Range &meta)> EpochIsEqual;
private:
    uint64_t id_{0};
    uint64_t applied_{0};
    uint64_t curr_seq_{0};
    uint64_t curr_index_{0};

    raft::impl::RaftServerImpl* server_ = nullptr;
    std::shared_ptr<impl::storage::DiskStorage> storage_ = nullptr;

    std::vector<std::shared_ptr<LogFile>> log_files_;
    //<seq, applied_index>
    std::unordered_map<uint64_t, uint64_t> done_files_;
    //rocksdb
    dataserver::storage::DbInterface* db_ == nullptr;
    std::atomic<bool> running_ = false;
    StorageThread* trd_ == nullptr;

    std::mutex  mtx_;
    std::condition_variable cond_;
};

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
