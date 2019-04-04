_Pragma("once");

#include <atomic>
#include <unordered_map>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <vector>
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "raft/server.h"

namespace sharkstore {
    class Status;
}

namespace sharkstore {
namespace raft {
namespace impl {
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
using RaftServerImpl = sharkstore::raft::impl::RaftServerImpl;
using RaftServer = sharkstore::raft::RaftServer;
using DiskStorage = sharkstore::raft::impl::storage::DiskStorage;
using LogFile = sharkstore::raft::impl::storage::LogFile;
using Entry = sharkstore::raft::impl::pb::Entry;
using RaftImpl =  sharkstore::raft::impl::RaftImpl;

namespace sharkstore {
namespace raft {

class RaftLogReader {
public:
    //range_id
    RaftLogReader(const uint64_t, const uint64_t, RaftServer *server) = default;
    virtual ~ RaftLogReader() = default;

    RaftLogReader(const RaftLogReader&) = delete;
    RaftLogReader& operator=(const RaftLogReader&) = delete;

    virtual Status GetData(const uint64_t idx, std::shared_ptr<raft_cmdpb::Command>& cmd) = 0;
    virtual Status Close() = 0;

};

std::shared_ptr<RaftLogReader> CreateRaftLogReader(
        const uint64_t id,
        const uint64_t start_idx,
        RaftServer *server);

} /* namespace raft */
} /* namespace sharkstore */
