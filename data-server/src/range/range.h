_Pragma("once");

#include <rocksdb/db.h>
#include <stdint.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>

#include "frame/sf_logger.h"
#include "frame/sf_util.h"

#include "base/shared_mutex.h"
#include "base/status.h"
#include "base/util.h"

#include "common/generater.h"
#include "common/ds_encoding.h"
#include "storage/store.h"

#include "raft/raft.h"
#include "raft/statemachine.h"
#include "raft/types.h"

#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/watchpb.pb.h"

#include "server/context_server.h"
#include "server/run_status.h"
#include "watch.h"

#include "meta_keeper.h"

namespace sharkstore {
namespace dataserver {
namespace range {

const int DEFAULT_LOCK_DELETE_TIME_MILLSEC = 3000;
enum {
    LOCK_OK = 0,
    LOCK_EXISTED,
    LOCK_NOT_EXIST,
    LOCK_ID_MISMATCHED,
    LOCK_IS_FORCE_UNLOCKED,
    LOCK_STORE_FAILED,
    LOCK_EPOCH_ERROR
};

class Range : public raft::StateMachine, public std::enable_shared_from_this<Range> {
public:
    Range(server::ContextServer *context, const metapb::Range &meta);
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

    // lock
    kvrpcpb::LockValue *LockGet(const std::string &key);
    void Lock(common::ProtoMessage *msg, kvrpcpb::DsLockRequest &req);
    void LockUpdate(common::ProtoMessage *msg, kvrpcpb::DsLockUpdateRequest &req);
    void Unlock(common::ProtoMessage *msg, kvrpcpb::DsUnlockRequest &req);
    void UnlockForce(common::ProtoMessage *msg, kvrpcpb::DsUnlockForceRequest &req);
    void LockWatch(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req);
    void LockScan(common::ProtoMessage *msg, kvrpcpb::DsLockScanRequest &req);

    // KV
    void RawGet(common::ProtoMessage *msg, kvrpcpb::DsKvRawGetRequest &req);
    void RawPut(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    void RawDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);

    void Insert(common::ProtoMessage *msg, kvrpcpb::DsInsertRequest &req);
    void Select(common::ProtoMessage *msg, kvrpcpb::DsSelectRequest &req);
    void Delete(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);
    
    void KVSet(common::ProtoMessage *msg, kvrpcpb::DsKvSetRequest &req);
    void KVGet(common::ProtoMessage *msg, kvrpcpb::DsKvGetRequest &req);
    void KVBatchSet(common::ProtoMessage *msg, kvrpcpb::DsKvBatchSetRequest &req);
    void KVBatchGet(common::ProtoMessage *msg, kvrpcpb::DsKvBatchGetRequest &req);
    void KVDelete(common::ProtoMessage *msg, kvrpcpb::DsKvDeleteRequest &req);
    void KVBatchDelete(common::ProtoMessage *msg, kvrpcpb::DsKvBatchDeleteRequest &req);
    void KVRangeDelete(common::ProtoMessage *msg, kvrpcpb::DsKvRangeDeleteRequest &req);
    void KVScan(common::ProtoMessage *msg, kvrpcpb::DsKvScanRequest &req);

    //KV watch series
    Status GetAndResp(const common::ProtoMessage *, watchpb::DsWatchRequest &, std::string &,
                      std::string &, watchpb::DsWatchResponse *, uint64_t &, errorpb::Error *);
    void WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req);
    void PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req);
    void WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req);
    void WatchDel(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req);
    bool WatchPutSubmit(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req);
    bool WatchDeleteSubmit(common::ProtoMessage *msg,
                            watchpb::DsKvWatchDeleteRequest &req);

public:
    kvrpcpb::KvRawGetResponse *RawGetResp(const std::string &key);
    kvrpcpb::SelectResponse *SelectResp(const kvrpcpb::DsSelectRequest &req);
    // RawPutSubmit cannot be called repeatedly
    bool RawPutSubmit(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    // RawDeleteSubmit cannot be called repeatedly
    bool RawDeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);
    // DeleteSubmit cannot be called repeatedly
    bool DeleteSubmit(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);

private:
    struct AsyncContext {
        AsyncContext(raft_cmdpb::CmdType type, server::ContextServer *cs,
                     common::ProtoMessage *msg, kvrpcpb::RequestHeader *req)
            : cmd_type_(type),
              context_server(cs),
              proto_message(msg),
              request_header(req) {}

        ~AsyncContext() {
            if (proto_message != nullptr) delete proto_message;
            if (request_header != nullptr) delete request_header;

            if (submit_time > 0) {
                context_server->run_status->PushTime(monitor::PrintTag::Raft,
                                                     get_micro_second() - submit_time);
            }
        }
        common::ProtoMessage *release_proto_message() {
            auto msg = proto_message;
            proto_message = nullptr;
            return msg;
        }
        kvrpcpb::RequestHeader *release_request_header() {
            auto rh = request_header;
            request_header = nullptr;
            return rh;
        }
        raft_cmdpb::CmdType cmd_type_;
        server::ContextServer *context_server = nullptr;
        common::ProtoMessage *proto_message = nullptr;
        kvrpcpb::RequestHeader *request_header = nullptr;

        uint64_t submit_time = 0;
    };

    AsyncContext *AddContext(uint64_t id, raft_cmdpb::CmdType type,
                             common::ProtoMessage *msg, kvrpcpb::RequestHeader *req);
    AsyncContext *ReleaseContext(uint64_t seq_id);

    void DelContext(uint64_t seq_id);
    void ClearExpiredContext();
    std::tuple<bool, uint64_t> GetExpiredContext();

    void SendTimeOutError(AsyncContext *context);

private:
    kvrpcpb::KvRawGetResponse *RawGetTry(const std::string &key);
    kvrpcpb::SelectResponse *SelectTry(const kvrpcpb::DsSelectRequest &req);
    bool RawPutTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawPutRequest &req);
    bool RawDeleteTry(common::ProtoMessage *msg, kvrpcpb::DsKvRawDeleteRequest &req);
    bool DeleteTry(common::ProtoMessage *msg, kvrpcpb::DsDeleteRequest &req);
    
private:
    Status Submit(const raft_cmdpb::Command &cmd);
    Status Apply(const raft_cmdpb::Command &cmd, uint64_t index);

    Status ApplyRawPut(const raft_cmdpb::Command &cmd);
    Status ApplyRawDelete(const raft_cmdpb::Command &cmd);

    Status ApplyWatchPut(const raft_cmdpb::Command &cmd);
    Status ApplyWatchDel(const raft_cmdpb::Command &cmd);

    Status ApplyInsert(const raft_cmdpb::Command &cmd);
    Status ApplyDelete(const raft_cmdpb::Command &cmd);

    Status ApplySplit(const raft_cmdpb::Command &cmd, uint64_t index);

    Status ApplyAddPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyDelPeer(const raft::ConfChange &cc, bool *updated);
    Status ApplyPromotePeer(const raft::ConfChange &cc, bool *updated);

    Status ApplyKVSet(const raft_cmdpb::Command &cmd);
    Status ApplyKVBatchSet(const raft_cmdpb::Command &cmd);
    Status ApplyKVDelete(const raft_cmdpb::Command &cmd);
    Status ApplyKVBatchDelete(const raft_cmdpb::Command &cmd);
    Status ApplyKVRangeDelete(const raft_cmdpb::Command &cmd);

    Status ApplyLock(const raft_cmdpb::Command &cmd);
    Status ApplyLockUpdate(const raft_cmdpb::Command &cmd);
    Status ApplyUnlock(const raft_cmdpb::Command &cmd);
    Status ApplyUnlockForce(const raft_cmdpb::Command &cmd);

    // split func
    void CheckSplit(uint64_t size);
    void AskSplit(std::string &key, metapb::Range *meta);
    void ReportSplit(const metapb::Range &new_range);

    int64_t checkMaxCount(int64_t maxCount) {
        if (maxCount <= 0) maxCount = std::numeric_limits<int64_t>::max();
        if (maxCount > max_count_) {
            //FLOG_WARN("%ld exceeded maxCount(%ld)", maxCount, max_count_);
            maxCount = max_count_;
        }
        return maxCount;
    }

    template <class R>
    void SendError(AsyncContext *context, R *resp, errorpb::Error *err) {
        auto header = resp->mutable_header();

        context_->socket_session->SetResponseHeader(*context->request_header, header,
                                                    err);
        context_->socket_session->Send(context->release_proto_message(), resp);
    }

    template <class R>
    void SendError(common::ProtoMessage *msg, const kvrpcpb::RequestHeader &req, R *resp,
                   errorpb::Error *err) {
        auto header = resp->mutable_header();

        context_->socket_session->SetResponseHeader(req, header, err);
        context_->socket_session->Send(msg, resp);
    }

    template <class RequestT>
    Status SubmitCmd(common::ProtoMessage *msg, RequestT &req,
                     const std::function<void(raft_cmdpb::Command &cmd)> &init) {
        raft_cmdpb::Command cmd;
        uint64_t seq_id = submit_seq_.fetch_add(1);

        cmd.mutable_cmd_id()->set_node_id(node_id_);
        cmd.mutable_cmd_id()->set_seq(seq_id);
        init(cmd);

        // set verify epoch
        auto epoch = new metapb::RangeEpoch(req.header().range_epoch());
        cmd.set_allocated_verify_epoch(epoch);

        auto context = AddContext(seq_id, cmd.cmd_type(), msg, req.release_header());
        context->submit_time = get_micro_second();

        auto ret = Submit(cmd);
        if (!ret.ok()) {
            context->release_proto_message();
            req.set_allocated_header(context->release_request_header());
            DelContext(seq_id);
        }

        return ret;
    }

    template <class ResponseT>
    Status SendResponse(ResponseT *response, const raft_cmdpb::Command &cmd, int code,
                        errorpb::Error *err) {
        std::unique_ptr<AsyncContext> context(ReleaseContext(cmd.cmd_id().seq()));
        if (context == nullptr) {
            //FLOG_ERROR("Apply cmd id %" PRIu64 " not found", cmd.cmd_id().seq());

            if (err != nullptr) {
                delete err;
            }

            delete response;
            return Status(Status::kTimedOut, CmdType_Name(cmd.cmd_type()) + " time out",
                          "");
        }

        auto etime = get_micro_second();
        auto take = etime - context->proto_message->begin_time;
        if (take > kTimeTakeWarnThresoldUSec) {
            auto method = funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(context->proto_message->header.func_id));
            FLOG_WARN("range[%lu] %s takes too long(%ld ms), sid=%ld, msgid=%ld", id_, method.c_str(), take / 1000,
                      context->proto_message->session_id, context->proto_message->header.msg_id);
        }

        FLOG_DEBUG("range[%lu] response msgid=%ld.", id_, context->proto_message->header.msg_id);

        response->mutable_resp()->set_code(code);

        context_->socket_session->SetResponseHeader(*context->request_header,
                                                    response->mutable_header(), err);
        context_->socket_session->Send(context->release_proto_message(), response);

        return Status::OK();
    }

    template <class ResponseT>
    Status SendResponse(ResponseT *response, const raft_cmdpb::Command &cmd, int code,
                        uint64_t rows, errorpb::Error *err) {
        response->mutable_resp()->set_affected_keys(rows);
        return SendResponse(response, cmd, code, err);
    }

public:
    // Admin
    void AdminSplit(mspb::AskSplitResponse &resp);

    void AddPeer(const metapb::Peer &peer);
    void DelPeer(const metapb::Peer &peer);

    void ResetStatisSize();
    void Heartbeat();

    Status Destroy();

    // get private member
public:
    bool valid() { return valid_; }
    metapb::Range options() const { return meta_.Get(); }
    bool EpochIsEqual(const metapb::Range &meta) {
        return EpochIsEqual(meta.range_epoch());
    };
    void set_real_size(uint64_t rsize) { real_size_ = rsize; }
    void GetReplica(metapb::Replica *rep);

    void setLeaderFlag(bool flag) {
        is_leader_ = flag;
    }

private:
    bool VerifyLeader(errorpb::Error *&err);
    bool CheckWriteable();
    bool KeyInRange(const std::string &key);
    bool KeyInRange(const std::string &key, errorpb::Error *&err);

    bool EpochIsEqual(const metapb::RangeEpoch &epoch);
    bool EpochIsEqual(const metapb::RangeEpoch &epoch, errorpb::Error *&);

    bool PushHeartBeatMessage();

    Status SaveMeta(const metapb::Range &meta);

    errorpb::Error *TimeOutError();
    errorpb::Error *RaftFailError();
    errorpb::Error *NoLeaderError();
    errorpb::Error *NotLeaderError(metapb::Peer &&peer);
    errorpb::Error *KeyNotInRange(const std::string &key);
    errorpb::Error *StaleEpochError(const metapb::RangeEpoch &epoch);

private:
    int32_t WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, std::string &errMsg);
    int64_t getcurrVersion(errorpb::Error *err) const {
        int64_t  version{0};
        if( 0 != version_seq_->currentId(&version)) {
            if (err == nullptr) {
                err = new errorpb::Error;
            }
            err->set_message(version_seq_->getErrMsg());
        }

        return version;
    }

    int64_t getNextVersion(errorpb::Error *err) const {
        int64_t  version{0};
        if( 0 != version_seq_->nextId(&version)) {
            if (err == nullptr) {
                err = new errorpb::Error;
            }
            err->set_message(version_seq_->getErrMsg());
        }

        return version;
    }


private:
    static const int kTimeTakeWarnThresoldUSec = 500000;

    server::ContextServer *context_ = nullptr;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    // cache range's start key
    // since it will not change unless we have merge operation
    const std::string start_key_;

    MetaKeeper meta_;

    std::atomic<bool> valid_ = { true };

    uint64_t apply_index_ = 0;
    std::atomic<bool> is_leader_ = {false};

    uint64_t real_size_ = 0;
    std::atomic<bool> statis_flag_ = {false};
    std::atomic<uint64_t> statis_size_ = {0};
    uint64_t split_range_id_ = 0;

    //std::atomic<uint64_t> version_seq_{0};
    sharkstore::IdGenerater *version_seq_ = nullptr;

    typedef std::pair<time_t, uint64_t> tr;
    std::atomic<uint64_t> submit_seq_{1};
    std::unordered_map<uint64_t, AsyncContext *> submit_map_;
    std::priority_queue<tr, std::vector<tr>, std::greater<tr>> submit_queue_;
    std::mutex submit_mutex_;

    storage::Store *store_ = nullptr;
    std::shared_ptr<raft::Raft> raft_ = nullptr;

    int64_t max_count_ = 1000;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
