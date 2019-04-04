_Pragma("once");

#include <memory>
#include <rocksdb/db.h>

#include "base/status.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "stats.h"
#include "split_policy.h"
#include "server/context_server.h"
#include "storage/db/db_interface.h"

namespace sharkstore {

namespace raft { class RaftServer; }

namespace dataserver {

namespace master { class Worker; }
namespace storage { class MetaStore; }
namespace watch { class WatchServer; }
namespace server { class PersistServer; }

namespace range {

class Range;

class RangeContext {
public:
    RangeContext() = default;
    virtual ~RangeContext() = default;

    virtual uint64_t GetNodeID() const = 0;

    // 分裂策略
    virtual SplitPolicy* GetSplitPolicy() = 0;

    virtual storage::DbInterface* DBInstance(const uint64_t flag = 0) = 0;
    virtual master::Worker* MasterClient() = 0;
    virtual raft::RaftServer* RaftServer() = 0;
    virtual storage::MetaStore* MetaStore() = 0;
    virtual RangeStats* Statistics(const uint64_t flag = 0) = 0;
    virtual watch::WatchServer* WatchServer() = 0;
    virtual server::PersistServer* PersistServer() = 0;

    // filesystem usage percent for check writable
    virtual uint64_t GetFSUsagePercent(const uint64_t seq) const = 0;

    virtual void ScheduleHeartbeat(uint64_t range_id, bool delay) = 0;
    virtual void ScheduleCheckSize(uint64_t range_id) = 0;

    // range manage
    virtual std::shared_ptr<Range> FindRange(uint64_t range_id) = 0;

    // split
    virtual Status SplitRange(uint64_t range_id,
            const raft_cmdpb::SplitRequest &req, uint64_t raft_index) = 0;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
