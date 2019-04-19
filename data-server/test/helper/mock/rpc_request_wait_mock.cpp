#include "rpc_request_wait_mock.h"

#include "base/util.h"

namespace sharkstore {
namespace test {
namespace mock {

void RPCFutureWait::Set(const google::protobuf::Message& resp) {
    if (replied_) {
        throw std::logic_error("already replied");
    }
    replied_ = true;
    resp.SerializeToString(&reply_data_);
    cv_.notify_all();
}

Status RPCFutureWait::Get(google::protobuf::Message& resp) {

    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait_for(lk, std::chrono::seconds(3)); 

    if (!replied_) {
        return Status(Status::kNotFound, "not reply yet", "");
    }

    if (!resp.ParseFromString(reply_data_)) {
        return Status(Status::kCorruption, "parse proto response", EncodeToHex(reply_data_));
    }
    return Status::OK();
}


RPCRequestWaitMock::RPCRequestWaitMock(const google::protobuf::Message& req,
               const RPCFutureWaitPtr& future,
               funcpb::FunctionID func_id):
    RPCRequest(net::Context(), net::NewMessage()),
    future_(future) {
    msg->head.func_id = func_id;
    auto &body = msg->body;
    body.resize(req.ByteSizeLong());
    req.SerializeToArray(body.data(), static_cast<int>(body.size()));
}

void RPCRequestWaitMock::Reply(const google::protobuf::Message& proto_resp) {
    future_->Set(proto_resp);
}


std::pair<RPCRequestPtr, RPCFutureWaitPtr> NewMockRPCRequestWait(
        const google::protobuf::Message& proto_req,
        funcpb::FunctionID func_id) {
    auto future = std::make_shared<RPCFutureWait>();
    RPCRequestPtr rpc(new RPCRequestWaitMock(proto_req, future, func_id));
    return std::make_pair(std::move(rpc), future);
}

} // namespace mock
} // namespace test
} // namespace sharkstore
