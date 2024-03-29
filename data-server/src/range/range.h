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
#include "server/context_server.h"
#include "server/run_status.h"
#include "watch/watch_event_buffer.h"
#include "watch/watcher.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/txn.pb.h"

#include "meta_keeper.h"
#include "context.h"
#include "submit.h"
#include "range_logger.h"

// for test friend class
namespace sharkstore { namespace test { namespace helper { class RangeTestFixture; }}}

namespace sharkstore {
namespace dataserver {
namespace range {

static const int DEFAULT_LOCK_DELETE_TIME_MILLSEC = 3000;
enum {
    LOCK_OK = 0,
    LOCK_NOT_EXIST = Status::kNotFound,
    LOCK_EXISTED,
    LOCK_ID_MISMATCHED,
    LOCK_IS_FORCE_UNLOCKED,
    LOCK_STORE_FAILED,
    LOCK_EPOCH_ERROR,
    LOCK_TIME_OUT = Status::kTimedOut,  //value 7 same with defined in status.h
    LOCK_PARAMETER_ERROR
};

class Range : public raft::StateMachine, public std::enable_shared_from_this<Range> {
public:
    Range(RangeContext* context, const metapb::Range &meta);
    ~Range();

    Range(const Range &) = delete;
    Range &operator=(const Range &) = delete;
    Range &operator=(const Range &) volatile = delete;

    Status Initialize(uint64_t leader = 0, uint64_t log_start_index = 0);
    Status Shutdown();

    Status Apply(const std::string &cmd, uint64_t index) override;
    Status ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) override;

    void OnReplicateError(const std::string &cmd, const Status &status) override {};

    void OnLeaderChange(uint64_t leader, uint64_t term) override;

    std::shared_ptr<raft::Snapshot> GetSnapshot() override;
    Status ApplySnapshotStart(const std::string &context) override;
    Status ApplySnapshotData(const std::vector<std::string> &datas) override;
    Status ApplySnapshotFinish(uint64_t index) override;

    void TransferLeader();
    void GetPeerInfo(raft::RaftStatus *raft_status);
    uint64_t GetPeerID() const;

    Status ForceSplit(uint64_t version, std::string* split_key);

    // lock
    bool LockQuery(const std::string &key, kvrpcpb::LockValue* lock_value);
    void Lock(RPCRequestPtr rpc, kvrpcpb::DsLockRequest &req);
    void LockUpdate(RPCRequestPtr rpc, kvrpcpb::DsLockUpdateRequest &req);
    void Unlock(RPCRequestPtr rpc, kvrpcpb::DsUnlockRequest &req);
    void UnlockForce(RPCRequestPtr rpc, kvrpcpb::DsUnlockForceRequest &req);
    void LockWatch(RPCRequestPtr rpc, watchpb::DsWatchRequest& req);
    void LockScan(RPCRequestPtr rpc, kvrpcpb::DsLockScanRequest &req);
    void LockGet(RPCRequestPtr rpc, kvrpcpb::DsLockGetRequest &req);

    // KV
    void RawGet(RPCRequestPtr rpc_request, kvrpcpb::DsKvRawGetRequest &req);
    void RawPut(RPCRequestPtr rpc_request, kvrpcpb::DsKvRawPutRequest &req);
    void RawDelete(RPCRequestPtr rpc, kvrpcpb::DsKvRawDeleteRequest &req);

    void Insert(RPCRequestPtr rpc, kvrpcpb::DsInsertRequest &req);
    void Update(RPCRequestPtr rpc, kvrpcpb::DsUpdateRequest &req);
    void Select(RPCRequestPtr rpc, kvrpcpb::DsSelectRequest &req);
    void Delete(RPCRequestPtr rpc, kvrpcpb::DsDeleteRequest &req);

    // TXN
    void TxnPrepare(RPCRequestPtr rpc, txnpb::DsPrepareRequest& req);
    void TxnDecide(RPCRequestPtr rpc, txnpb::DsDecideRequest& req);
    void TxnClearup(RPCRequestPtr rpc, txnpb::DsClearupRequest& req);
    void TxnGetLockInfo(RPCRequestPtr rpc, txnpb::DsGetLockInfoRequest& req);
    void TxnSelect(RPCRequestPtr rpc, txnpb::DsSelectRequest& req);
    void TxnScan(RPCRequestPtr rpc, txnpb::DsScanRequest& req);

    //KV watch series
    Status GetAndResp(watch::WatcherPtr pWatcher, const watchpb::WatchCreateRequest& req, const std::string &dbKey,
            const bool &prefix, int64_t &version, watchpb::DsWatchResponse *dsResp);
    void WatchGet(RPCRequestPtr rpc, watchpb::DsWatchRequest &req); 
    void PureGet(RPCRequestPtr rpc, watchpb::DsKvWatchGetMultiRequest &req);
    void WatchPut(RPCRequestPtr rpc, watchpb::DsKvWatchPutRequest &req);
    void WatchDel(RPCRequestPtr rpc, watchpb::DsKvWatchDeleteRequest &req);

private:
    Status Submit(const raft_cmdpb::Command &cmd);
    void ClearExpiredContext();

    Status Apply(const raft_cmdpb::Command &cmd, uint64_t index);

    void rawGet(RPCRequestPtr rpc_request, kvrpcpb::DsKvRawGetRequest &req, bool redirect);
    void rawPut(RPCRequestPtr rpc_request, kvrpcpb::DsKvRawPutRequest &req, bool redirect);
    void rawDelete(RPCRequestPtr rpc, kvrpcpb::DsKvRawDeleteRequest &req, bool redirect);
    Status ApplyRawPut(const raft_cmdpb::Command &cmd);
    Status ApplyRawDelete(const raft_cmdpb::Command &cmd);

    Status ApplyWatchPut(const raft_cmdpb::Command &cmd, uint64_t raft_index);
    Status ApplyWatchDel(const raft_cmdpb::Command &cmd, uint64_t raft_index);

    void select(RPCRequestPtr rpc, kvrpcpb::DsSelectRequest &req, bool redirect);
    void deleteRow(RPCRequestPtr rpc, kvrpcpb::DsDeleteRequest &req, bool redirect);
    Status ApplyInsert(const raft_cmdpb::Command &cmd);
    Status ApplyUpdate(const raft_cmdpb::Command &cmd);
    Status ApplyDelete(const raft_cmdpb::Command &cmd);

    Status ApplySplit(const raft_cmdpb::Command &cmd, uint64_t index);

    Status ApplyAddPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyDelPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyPromotePeer(const raft::ConfChange &cc, bool *updated);

    Status ApplyLock(const raft_cmdpb::Command &cmd, uint64_t raft_index);
    Status ApplyLockUpdate(const raft_cmdpb::Command &cmd);
    Status ApplyUnlock(const raft_cmdpb::Command &cmd);
    Status ApplyUnlockForce(const raft_cmdpb::Command &cmd);

    Status ApplyTxnPrepare(const raft_cmdpb::Command &cmd, uint64_t raft_index);
    Status ApplyTxnDecide(const raft_cmdpb::Command &cmd, uint64_t raft_index);
    Status ApplyTxnClearup(const raft_cmdpb::Command &cmd, uint64_t raft_index);

    // split func
    void CheckSplit(uint64_t size);
    void AskSplit(std::string &&key, metapb::Range&& meta, bool force = false);
    void ReportSplit(const metapb::Range &new_range);


    int64_t checkMaxCount(int64_t maxCount) { 
        static const int64_t kDefaultKVMaxCount = 1000;
        if (maxCount <= 0)
            maxCount = std::numeric_limits<int64_t>::max();
        if (maxCount > kDefaultKVMaxCount) {
            maxCount = kDefaultKVMaxCount ;
        }
        return maxCount;
    }

    // 根据request的header设定response的header，然后发送response
    template <class ResponseT>
    void SendResponse(const RPCRequestPtr& rpc, ResponseT& resp,
            const kvrpcpb::RequestHeader &req, errorpb::Error *err = nullptr) {
        auto header = resp.mutable_header();
        SetResponseHeader(header, req, err);
        rpc->Reply(resp);
    }

    // 提交给raft，放入队列等Apply的时候再拿出来回应
    // 如果提交失败会从队列中删除，并发送错误回应
    template <class ResponseT>
    void SubmitCmd(RPCRequestPtr rpc, const kvrpcpb::RequestHeader& header,
                   const std::function<void(raft_cmdpb::Command &cmd)> &init) {
        raft_cmdpb::Command cmd;
        init(cmd);
        // set verify epoch
        auto epoch = new metapb::RangeEpoch(header.range_epoch());
        cmd.set_allocated_verify_epoch(epoch);
        // add to queue
        auto seq = submit_queue_.Add<ResponseT>(std::move(rpc), cmd.cmd_type(), header);
        cmd.mutable_cmd_id()->set_node_id(node_id_);
        cmd.mutable_cmd_id()->set_seq(seq);
        auto ret = Submit(cmd); // 提交给raft
        if (!ret.ok()) {
            RANGE_LOG_ERROR("raft submit failed: %s", ret.ToString().c_str());
            auto ctx = submit_queue_.Remove(seq);
            if (ctx != nullptr) {
                ctx->SendError(RaftFailError()); // 提交失败，发送错误回应
            }
        }
    }

    // 走raft的命令处理完，从SubmitQueue取出上下文进行回应
    template <class ResponseT>
    void ReplySubmit(const raft_cmdpb::Command& cmd, ResponseT& resp, errorpb::Error *err, int64_t apply_time) {
        auto ctx = submit_queue_.Remove(cmd.cmd_id().seq());
        if (ctx != nullptr) {
            context_->Statistics()->PushTime(monitor::HistogramType::kRaft, apply_time - ctx->SubmitTime());
            ctx->CheckExecuteTime(id_, kTimeTakeWarnThresoldUSec);
            ctx->FillResponseHeader(resp.mutable_header(), err);
            ctx->SendResponse(resp);
        } else {
            delete err;
            RANGE_LOG_WARN("Apply cmd id %" PRIu64 " not found", cmd.cmd_id().seq());
        }
    }

public:
    // Admin
    void AdminSplit(mspb::AskSplitResponse &resp);

    void AddPeer(const metapb::Peer &peer);
    void DelPeer(const metapb::Peer &peer);

    void ResetStatisSize();
    void ResetStatisSize(SplitKeyMode mode, uint64_t split_size, uint64_t max_size);
    void Heartbeat();

    Status Destroy();

    // get private member
public:
    bool valid() { return valid_; }
    metapb::Range options() const { return meta_.Get(); }
    bool EpochIsEqual(const metapb::Range &meta) {
        return EpochIsEqual(meta.range_epoch());
    };
    void SetRealSize(uint64_t rsize) { real_size_ = rsize; }
    void GetReplica(metapb::Replica *rep);
    uint64_t GetSplitRangeID() const { return split_range_id_; }
    size_t GetSubmitQueueSize() const { return submit_queue_.Size(); }

    void setLeaderFlag(bool flag) {
        is_leader_ = flag;
    }

private:
    bool VerifyLeader(errorpb::Error *&err);
    bool VerifyReadable(uint64_t read_index, errorpb::Error *&err);
    bool hasSpaceLeft(errorpb::Error **err = nullptr); // 检查是否磁盘空间已满
    bool KeyInRange(const std::string &key);
    bool KeyInRange(const std::string &key, errorpb::Error *&err);
    bool KeyInRange(const txnpb::PrepareRequest& req, const metapb::RangeEpoch& epoch,
            errorpb::Error** err);
    bool KeyInRange(const txnpb::DecideRequest& req, const metapb::RangeEpoch& epoch,
            errorpb::Error** err);
    bool EpochIsEqual(const metapb::RangeEpoch &epoch);
    bool EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&);

    bool PushHeartBeatMessage();

    Status SaveMeta(const metapb::Range &meta);

    errorpb::Error *RaftFailError();
    errorpb::Error *NoLeaderError();
    errorpb::Error *NotLeaderError(metapb::Peer &&peer);
    errorpb::Error *KeyNotInRange(const std::string &key);
    errorpb::Error *StaleEpochError(const metapb::RangeEpoch &epoch);
    errorpb::Error *StaleReadIndexError(uint64_t read_index, uint64_t current_index);

private:
    friend class ::sharkstore::test::helper::RangeTestFixture;

    int32_t WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, const int64_t &version,
            std::string &errMsg, bool prefix = false);
    int32_t SendNotify( watch::WatcherPtr& w, watchpb::DsWatchResponse *ds_resp, bool prefix = false);

private:
    static const int kTimeTakeWarnThresoldUSec = 500000;

    RangeContext* context_ = nullptr;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    // cache range's start key
    // since it will not change unless we have merge operation
    const std::string start_key_;

    MetaKeeper meta_;

    std::atomic<bool> valid_ = { true };

    uint64_t apply_index_ = 0;
    bool save_apply_index_ = {true};
    std::atomic<bool> is_leader_ = {false};

    uint64_t real_size_ = 0;
    std::atomic<bool> statis_flag_ = {false};
    std::atomic<uint64_t> statis_size_ = {0};
    uint64_t split_range_id_ = 0;

    watch::CEventBuffer *eventBuffer = nullptr;
    SubmitQueue submit_queue_;

    std::unique_ptr<storage::Store> store_;
    std::shared_ptr<raft::Raft> raft_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
