#include "submit.h"

#include "base/util.h"
#include "frame/sf_logger.h"
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

void SubmitContext::FillResponseHeader(kvrpcpb::ResponseHeader* header, errorpb::Error *err) {
    SetResponseHeader(header, cluster_id_, trace_id_, err);
}

void SubmitContext::SendResponse(const ::google::protobuf::Message& resp) {
    rpc_request_->Reply(resp);
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
    auto now = NowMilliSeconds();
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
