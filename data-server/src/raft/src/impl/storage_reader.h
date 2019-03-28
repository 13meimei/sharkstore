_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include <functional>
//#include <mutex>
//#include <condition_variable>
#include <vector>

#include "raft/raft_log_reader.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/db_interface.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

namespace sharkstore {
    class Status;
namespace dataserver {
    class WorkThread;
namespace storage {
    static const std::string kPersistRaftLogPrefix = "\x06";

    class DbInterface;
}}}

using Status = sharkstore::Status;
using WorkThread =  sharkstore::dataserver::WorkThread;
using RaftServer = sharkstore::raft::RaftServer;
using LogFile = sharkstore::raft::impl::storage::LogFile;
using Entry = sharkstore::raft::impl::pb::Entry;
using DbInterface = sharkstore::dataserver::storage::DbInterface;
using RaftImpl =  sharkstore::raft::impl::RaftImpl;

namespace sharkstore {
namespace raft {
namespace impl {

class StorageReader : public RaftLogReader,  public std::enable_shared_from_this<StorageReader> {
public:
    StorageReader(const uint64_t id,
              const std::function<bool(const std::string&)>& f0,
              const std::function<bool(const metapb::RangeEpoch&)>& f1,
              RaftServer *server,
              DbInterface* db,
              WorkThread* trd);
    ~ StorageReader() override ;

    StorageReader(const StorageReader&) = delete;
    StorageReader& operator=(const StorageReader&) = delete;

    Status Run() override ;
    size_t GetCommitFiles() override ;
    Status ProcessFiles() override ;

    Status ApplyRaftCmd(const raft_cmdpb::Command& cmd) override ;
    Status StoreAppliedIndex(const uint64_t& seq, const uint64_t& index) override ;
    Status AppliedTo(uint64_t index) override ;
    //Status ApplySnapshot(const pb::SnapshotMeta&meta);

    uint64_t Applied()  override;

    //index: range applied index
    Status Notify(const uint64_t range_id, const uint64_t index) override ;
    Status Close() override ;

private:
    using EntryPtr = std::shared_ptr<Entry>;
    std::shared_ptr<raft_cmdpb::Command> decodeEntry(EntryPtr entry);
    Status storeRawPut(const raft_cmdpb::Command &cmd);
    Status saveApplyIndex(uint64_t range_id, uint64_t apply_index);

    bool tryPost(const std::function<void()>& f);
    Status listLogs();

    std::function<bool(const std::string& key)> keyInRange;
    std::function<bool(const metapb::RangeEpoch& meta)> EpochIsEqual;
private:
    uint64_t id_{0};
    uint64_t applied_{0};
    uint64_t curr_seq_{0};
    uint64_t curr_index_{0};

    sharkstore::raft::RaftServer* server_ = nullptr;

    std::vector<std::shared_ptr<LogFile>> log_files_;
    //<seq, applied_index>
    std::unordered_map<uint64_t, uint64_t> done_files_;
    //rocksdb
    DbInterface* db_ = nullptr;
    std::atomic<bool> running_ = {false};
    WorkThread* trd_ = nullptr;

    //std::mutex  mtx_;
    //std::condition_variable cond_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
