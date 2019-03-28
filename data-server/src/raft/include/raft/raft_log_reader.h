_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <vector>

//#include "../../raft.pb.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/db_interface.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

namespace sharkstore {
    class Status;
namespace dataserver {
namespace storage {
    static const std::string kPersistRaftLogPrefix = "\x06";

    class DbInterface;
}}}

namespace sharkstore {
namespace raft {
namespace impl {
    class Work;

    class WorkThread;

    class RaftServerImpl;

    class RaftImpl;
    namespace pb {
        class Entry;
    }

    namespace storage {
        class DiskStorage;
        class LogFile;
    }
}}}

using Status = sharkstore::Status;
using StorageThread =  sharkstore::raft::impl::WorkThread;
using RaftServerImpl = sharkstore::raft::impl::RaftServerImpl;
using DiskStorage = sharkstore::raft::impl::storage::DiskStorage;
using LogFile = sharkstore::raft::impl::storage::LogFile;
using Entry = sharkstore::raft::impl::pb::Entry;
using DbInterface = sharkstore::dataserver::storage::DbInterface;
using RaftImpl =  sharkstore::raft::impl::RaftImpl;

namespace sharkstore {
namespace raft {

class RaftLogReader {
public:
    RaftLogReader(const uint64_t id,
              const std::function<bool(const std::string&)>& f0,
              const std::function<bool(const metapb::Range &meta)>& f1,
              RaftServerImpl *server,
              sharkstore::dataserver::storage::DbInterface* db,
              sharkstore::raft::impl::WorkThread* trd);
    virtual ~ RaftLogReader();

    RaftLogReader(const RaftLogReader&) = delete;
    RaftLogReader& operator=(const RaftLogReader&) = delete;

    virtual Status Run() = 0;
    virtual size_t GetCommitFiles() = 0;
    virtual Status ProcessFiles() = 0;

    virtual Status ApplyRaftCmd(const raft_cmdpb::Command& cmd) = 0;
    virtual Status StoreAppliedIndex(const uint64_t& seq, const uint64_t& index) = 0;
    virtual Status AppliedTo(uint64_t index) = 0;
    //Status ApplySnapshot(const pb::SnapshotMeta&meta);

    virtual uint64_t Applied() = 0;

    //index: range applied index
    virtual Status Notify(const uint64_t range_id, const uint64_t index) = 0;
    virtual Status Close() = 0;

};

std::unique_ptr<RaftLogReader> CreateRaftLogReader(const uint64_t id,
                                                   const std::function<bool(const std::string&)>& f0,
                                                   const std::function<bool(const metapb::Range &meta)>& f1,
                                                   RaftServerImpl *server,
                                                   sharkstore::dataserver::storage::DbInterface* db,
                                                   sharkstore::raft::impl::WorkThread* trd);

} /* namespace raft */
} /* namespace sharkstore */
