#include "rpc_request.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "base/util.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {

RPCRequest::RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg) :
    ctx(req_ctx),
    msg(req_msg),
    begin_time(NowMicros()) {
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

void SetResponseHeader(const kvrpcpb::RequestHeader &req,
                       kvrpcpb::ResponseHeader *resp,
                       errorpb::Error *err) {
    resp->set_cluster_id(req.cluster_id());
    resp->set_trace_id(req.trace_id());
    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}

}  // namespace dataserver
}  // namespace sharkstore
