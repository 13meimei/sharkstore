#include "submit.h"

#include "base/util.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace range {


SubmitContext::SubmitContext(RPCRequestPtr rpc_request,
        raft_cmdpb::CmdType type, const kvrpcpb::RequestHeader& kv_header) :
    submit_time_(NowMicros()),
    rpc_request_(std::move(rpc_request)),
    type_(type),
    cluster_id_(kv_header.cluster_id()),
    trace_id_(kv_header.trace_id()) {
}

void SubmitContext::SendError(errorpb::Error *err) {
    switch (type_) {
        case raft_cmdpb::CmdType::RawPut: {
            kvrpcpb::DsKvRawPutResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::RawDelete: {
            kvrpcpb::DsKvRawDeleteResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::Insert: {
            kvrpcpb::DsInsertResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::Delete: {
            kvrpcpb::DsDeleteResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::Lock: {
            kvrpcpb::DsLockResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::LockUpdate: {
            kvrpcpb::DsLockUpdateResponse  resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::Unlock: {
            kvrpcpb::DsUnlockResponse  resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::UnlockForce: {
            kvrpcpb::DsUnlockForceResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::TxnPrepare: {
            txnpb::DsPrepareResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::TxnDecide: {
           txnpb::DsDecideResponse resp;
            Reply(resp, err);
            break;
        }
        case raft_cmdpb::CmdType::TxnClearup: {
            txnpb::DsClearupResponse resp;
            Reply(resp, err);
            break;
        }
        default:
            FLOG_ERROR("SubmitContext::SendTimeout: unknown cmd type: %d", static_cast<int>(type_));
            delete err;
    }
}

void SubmitContext::SendTimeout() {
    auto err = new errorpb::Error;
    err->set_message("request timeout");
    err->mutable_timeout();
    SendError(err);
}

void SubmitContext::CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs) {
    auto take = NowMicros() - rpc_request_->begin_time;
    if (take > thresold_usecs) {
        auto func_id = rpc_request_->msg->head.func_id;
        const auto& method = funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(func_id));
        auto msg_id = rpc_request_->msg->head.msg_id;

        FLOG_WARN("range[%" PRIu64 "] %s takes too long(%" PRId64 " ms), from=%s, msgid=%" PRId64,
                rangeID, method.c_str(), take / 1000, rpc_request_->ctx.remote_addr.c_str(), msg_id);
    }
}

uint64_t SubmitQueue::GetSeq() {
    std::lock_guard<std::mutex> lock(mu_);
    return ++seq_;
}

uint64_t SubmitQueue::Add(RPCRequestPtr rpc_request, raft_cmdpb::CmdType type,
             const kvrpcpb::RequestHeader& kv_req_header) {
    SubmitContextPtr ctx(new SubmitContext(std::move(rpc_request), type, kv_req_header));
    std::lock_guard<std::mutex> lock(mu_);
    ctx_map_.emplace(++seq_, std::move(ctx));
    expire_que_.emplace(rpc_request->expire_time, seq_);
    return seq_;
}

std::unique_ptr<SubmitContext> SubmitQueue::Remove(uint64_t seq_id) {
    std::unique_ptr<SubmitContext> ret;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ctx_map_.find(seq_id);
    if (it != ctx_map_.end()) {
        ret = std::move(it->second);
        ctx_map_.erase(it);
    }
    return ret;
}

std::vector<uint64_t> SubmitQueue::GetExpired(size_t max_count) {
    std::vector<uint64_t> result;
    auto now = getticks();
    {
        std::lock_guard<std::mutex> lock(mu_);
        while (!expire_que_.empty() && result.size() < max_count) {
            auto& p = expire_que_.top();
            if (p.first < now) {
                result.push_back(p.second);
                expire_que_.pop();
            } else {
                break;
            }
        }
    }
    return result;
}

size_t SubmitQueue::Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ctx_map_.size();
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
