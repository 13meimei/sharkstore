_Pragma("once");

#include <memory>
#include <rocksdb/db.h>

#include "statistics.h"
#include "base/status.h"

namespace sharkstore {

namespace raft { class RaftServer; }

namespace dataserver {

namespace master { class Worker; }
namespace storage { class MetaStore; }
namespace common { class SocketSession; }

namespace range {

class Range;

class RangeContext {
public:
    RangeContext() = default;
    virtual ~RangeContext() = default;

    // split parameters
    virtual uint64_t SplitSize() const = 0;
    virtual uint64_t MaxSize() const = 0;

    virtual uint64_t GetNodeID() const = 0;

    virtual rocksdb::DB *DBInstance() const() = 0;

    virtual master::Worker* MasterClient() = 0;
    virtual raft::RaftServer* RaftServer() = 0;
    virtual storage::MetaStore* MetaStore() = 0;
    virtual common::SocketSession* SocketSession() = 0;
    virtual Statistics* Statistics() = 0;

    virtual void ScheduleHeartbeat(uint64_t range_id, bool delay) {}
    virtual void ScheduleCheckSize(uint64_t range_id) {}

    // range manage
    virtual std::shared_ptr<Range> FindRange(uint64_t range_id) { return nullptr; }

    // split
    Status SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req,
            uint64_t raft_index, std::shared_ptr<Range> *new_range) {
        return Status(Status::kNotSupported);
    }
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
