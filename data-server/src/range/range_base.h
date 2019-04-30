_Pragma("once");

#include <stdint.h>
#include <atomic>
#include <string>

#include "frame/sf_logger.h"

#include "base/shared_mutex.h"
#include "base/util.h"

#include "common/ds_encoding.h"
#include "common/rpc_request.h"
#include "storage/store.h"
#include "raft/raft.h"
#include "raft/statemachine.h"
#include "raft/types.h"
#include "raft/raft_log_reader.h"
#include "server/context_server.h"
#include "server/run_status.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/txn.pb.h"

#include "meta_keeper.h"
#include "context.h"
#include "submit.h"

#include "watch/watch_event_buffer.h"
#include "watch/watcher.h"
// for test friend class
namespace sharkstore { namespace test { namespace helper { class RangeTestFixture; }}}

namespace sharkstore {
namespace dataserver {
namespace range {



int64_t checkMaxCount(int64_t maxCount);

struct ApplyStat {
    uint64_t write_bytes = 0;
};

class RangeBase {
public:
    RangeBase(RangeContext* context, const metapb::Range &meta);
    virtual ~RangeBase();

    RangeBase(const RangeBase &) = delete;
    RangeBase &operator=(const RangeBase &) = delete;
    RangeBase &operator=(const RangeBase &) volatile = delete;

    Status ApplyRawPut(const raft_cmdpb::Command &cmd, kvrpcpb::DsKvRawPutResponse& resp, ApplyStat& stat);
    Status ApplyRawDelete(const raft_cmdpb::Command &cmd, kvrpcpb::DsKvRawDeleteResponse& resp, ApplyStat& stat);

    Status ApplyInsert(const raft_cmdpb::Command &cmd, kvrpcpb::DsInsertResponse& resp, ApplyStat& stat);



public:
    bool VerifyWriteable(errorpb::Error **err = nullptr); // 检查是否磁盘空间已满
    bool KeyInRange(const std::string &key);
    bool KeyInRange(const std::string &key, errorpb::Error *&err);

    bool EpochIsEqual(const metapb::RangeEpoch &epoch);
    bool EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&);
    errorpb::Error *StaleEpochError(const metapb::RangeEpoch &epoch);
//    TXN new add
//    virtual bool KeyInRange(const txnpb::PrepareRequest& req, const metapb::RangeEpoch& epoch,
//                    errorpb::Error** err);
//    virtual bool KeyInRange(const txnpb::DecideRequest& req, const metapb::RangeEpoch& epoch,
//                    errorpb::Error** err);
    errorpb::Error* KeyNotInRange(const std::string &key);

protected:
    RangeContext* context_ = nullptr;
    const uint64_t node_id_;
    const uint64_t id_;
    const std::string start_key_;
    MetaKeeper meta_;

    std::unique_ptr<storage::Store> store_;
};


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
