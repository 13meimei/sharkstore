#include "rpc_request_mock.h"

#include "base/util.h"

namespace sharkstore {
namespace test {
namespace mock {

void RPCFuture::Set(const google::protobuf::Message& resp) {
    replied_ = true;
    resp.SerializeToString(&reply_data_);
}

Status RPCFuture::Get(google::protobuf::Message& resp) {
    if (!replied_) {
        return Status(Status::kNotFound, "not reply yet", "");
    }
    if (!resp.ParseFromString(reply_data_)) {
        return Status(Status::kCorruption, "parse proto response", EncodeToHex(reply_data_));
    }
    return Status::OK();
}


RPCRequestMock::RPCRequestMock(const google::protobuf::Message& req,
               const RPCFuturePtr& future,
               funcpb::FunctionID func_id):
    RPCRequest(net::Context(), net::NewMessage()),
    future_(future) {
    msg->head.func_id = func_id;
    auto &body = msg->body;
    body.resize(req.ByteSizeLong());
    req.SerializeToArray(body.data(), static_cast<int>(body.size()));
}

void RPCRequestMock::Reply(const google::protobuf::Message& proto_resp) {
    future_->Set(proto_resp);
}


std::pair<RPCRequestPtr, RPCFuturePtr> NewMockRPCRequest(
        const google::protobuf::Message& proto_req,
        funcpb::FunctionID func_id) {
    auto future = std::make_shared<RPCFuture>();
    RPCRequestPtr rpc(new RPCRequestMock(proto_req, future, func_id));
    return std::make_pair(std::move(rpc), future);
}

} // namespace mock
} // namespace test
} // namespace sharkstore
