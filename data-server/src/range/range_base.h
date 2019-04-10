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

//#include "watch/watch_event_buffer.h"
//#include "watch/watcher.h"
// for test friend class
namespace sharkstore { namespace test { namespace helper { class RangeTestFixture; }}}

namespace sharkstore {
namespace dataserver {
namespace range {

static const int64_t kDefaultKVMaxCount = 1000;

int64_t checkMaxCount(int64_t maxCount);

class RangeBase {
public:
    RangeBase(RangeContext* context, const metapb::Range &meta);
    virtual ~RangeBase();

    RangeBase(const RangeBase &) = delete;
    RangeBase &operator=(const RangeBase &) = delete;
    RangeBase &operator=(const RangeBase &) volatile = delete;

    virtual Status Initialize(uint64_t leader = 0, uint64_t log_start_index = 0, uint64_t sflag = 0);
    virtual Status Shutdown();

    //virtual Status Apply(const std::string &cmd, uint64_t index) { return Status::OK(); };
    virtual Status Apply(const raft_cmdpb::Command &cmd, uint64_t index);
public:
    virtual Status ApplyRawPut(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        return ApplyRawPut(cmd, err);
    };
    virtual Status ApplyRawDelete(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        return ApplyRawDelete(cmd, err);
    };

    virtual Status ApplyRawPut(const raft_cmdpb::Command &cmd, errorpb::Error *&err);
    virtual Status ApplyRawDelete(const raft_cmdpb::Command &cmd, errorpb::Error *&err);

    virtual Status ApplyInsert(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyInsert(cmd, affected_keys, err);
    };
    virtual Status ApplyUpdate(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyUpdate(cmd, affected_keys, err);
    };
    virtual Status ApplyDelete(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyDelete(cmd, affected_keys, err);
    };

    virtual Status ApplyInsert(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
    virtual Status ApplyUpdate(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
    virtual Status ApplyDelete(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);

    virtual Status ApplyKVSet(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyKVSet(cmd, affected_keys, err);
    };
    virtual Status ApplyKVBatchSet(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyKVBatchSet(cmd, affected_keys, err);
    };
    virtual Status ApplyKVDelete(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        return ApplyKVDelete(cmd, err);
    };
    virtual Status ApplyKVBatchDelete(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyKVBatchDelete(cmd, affected_keys, err);
    };
    virtual Status ApplyKVRangeDelete(const raft_cmdpb::Command &cmd) {
        errorpb::Error *err = nullptr;
        uint64_t affected_keys{0};
        return ApplyKVRangeDelete(cmd, affected_keys, err);
    };

    virtual Status ApplyKVSet(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
    virtual Status ApplyKVBatchSet(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
    virtual Status ApplyKVDelete(const raft_cmdpb::Command &cmd, errorpb::Error *&);
    virtual Status ApplyKVBatchDelete(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
    virtual Status ApplyKVRangeDelete(const raft_cmdpb::Command &cmd, uint64_t& , errorpb::Error *&);
public:
    virtual Status Destroy();

    virtual void CheckSplit(uint64_t size);
public:
    virtual bool valid() { return valid_; }
    virtual metapb::Range options() const { return meta_.Get(); }
    virtual bool EpochIsEqual(const metapb::Range &meta) {
        return EpochIsEqual(meta.range_epoch());
    };
    virtual void SetRealSize(uint64_t rsize) { real_size_ = rsize; }
    //virtual void GetReplica(metapb::Replica *rep);
    virtual uint64_t GetSplitRangeID() const { return split_range_id_; }

    virtual void setLeaderFlag(bool flag) {
        is_leader_ = flag;
    }
public:
    virtual bool VerifyWriteable(errorpb::Error **err = nullptr); // 检查是否磁盘空间已满
    virtual bool KeyInRange(const std::string &key);
    virtual bool KeyInRange(const std::string &key, errorpb::Error *&err);

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
    static const int kTimeTakeWarnThresoldUSec = 500000;

    RangeContext* context_ = nullptr;
    const uint64_t node_id_;
    const uint64_t id_;
    // cache range's start key
    // since it will not change unless we have merge operation
    const std::string start_key_;
    //1 slave range  0 master range(default)
    uint64_t slave_flag_{0};
    MetaKeeper meta_;

    std::atomic<bool> valid_ = { true };

    //uint64_t apply_index_;
    //uint64_t persist_index_;
    std::atomic<bool> is_leader_ = {false};

    uint64_t real_size_;
    std::atomic<bool> statis_flag_ = {false};
    std::atomic<uint64_t> statis_size_ = {0};
    uint64_t split_range_id_ = 0;

    std::unique_ptr<storage::Store> store_;

};


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
