#include "rpc_request.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "base/util.h"
#include "frame/sf_logger.h"
#include "proto/gen/funcpb.pb.h"

namespace sharkstore {

RPCRequest::RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg) :
    ctx(req_ctx),
    msg(req_msg),
    begin_time(NowMicros()) {
    expire_time = NowMilliSeconds();
    if (msg->head.timeout != 0) {
        expire_time += msg->head.timeout;
    } else {
        expire_time += kDefaultRPCRequestTimeoutMS;
    };
}

std::string RPCRequest::FuncName() const {
    return funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(msg->head.func_id));
}

bool RPCRequest::ParseTo(google::protobuf::Message& proto_req, bool zero_copy) {
    auto data = msg->body.data();
    auto len = static_cast<int>(msg->body.size());
    if (zero_copy) {
        google::protobuf::io::ArrayInputStream input(data, len);
        return proto_req.ParseFromZeroCopyStream(&input);
    } else {
        return proto_req.ParseFromArray(data, len);
    }
}

void RPCRequest::Reply(const google::protobuf::Message& proto_resp) {
    std::vector<uint8_t> resp_body;
    resp_body.resize(proto_resp.ByteSizeLong());
    auto ret = proto_resp.SerializeToArray(resp_body.data(), static_cast<int>(resp_body.size()));
    if (ret) {
        ret = ctx.Write(msg->head, std::move(resp_body));
        if (!ret) {
            FLOG_WARN("reply to %s failed: maybe connection is closed.", ctx.remote_addr.c_str());
        }
    } else {
        FLOG_ERROR("serialize response failed, msg: %s", proto_resp.ShortDebugString().c_str());
    }
}



void SetResponseHeader(kvrpcpb::ResponseHeader* resp,
                       const kvrpcpb::RequestHeader &req,
                       errorpb::Error *err) {
    SetResponseHeader(resp, req.cluster_id(), req.cluster_id(), err);
}

void SetResponseHeader(kvrpcpb::ResponseHeader* resp,
                       uint64_t cluster_id, uint64_t trace_id,
                       errorpb::Error *err) {
    resp->set_cluster_id(cluster_id);
    resp->set_trace_id(trace_id);
    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}

}  // namespace sharkstore
