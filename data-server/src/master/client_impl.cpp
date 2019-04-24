#include "client_impl.h"

#include <thread>

#include "frame/sf_logger.h"
#include "rpc_types.h"

namespace sharkstore {
namespace dataserver {
namespace master {

using namespace mspb;

ClientImpl::ClientImpl(const std::vector<std::string>& ms_addrs)
    : ms_addrs_(ms_addrs.cbegin(), ms_addrs.cend()),
      last_async_ask_(std::chrono::steady_clock::now()) {
}

ClientImpl::~ClientImpl() { cq_.Shutdown(); }


Status ClientImpl::Start(ResponseHandler* handler) {
    if (ms_addrs_.empty()) {
        return Status(Status::kInvalidArgument, "invalid master address", "empty");
    }

    std::thread thr(std::bind(&ClientImpl::recvResponse, this, handler));
    thr.detach();
    return Status::OK();
}

Status ClientImpl::Stop() {
    return Status::OK();
}

template <class RequestT, class ResponseT, class MethodFunc>
Status ClientImpl::SyncCall(const RequestT& req, const MethodFunc& func_ptr,
                                  size_t timeout_ms, ResponseT& resp) {
    // 获取连接
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    // 设置超时
    grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);

    auto status = (conn->GetStub().*func_ptr)(&context, req, &resp);
    checkStatus(status);
    if (!status.ok()) {
        return Status(Status::kUnknown,
                      status.error_message() + ", code=" + std::to_string(status.error_code()),
                      conn->GetAddr());
    } else if (checkResponseError(conn->GetAddr(), resp.header())) {
        return Status(Status::kNotLeader, "", conn->GetAddr());
    } else {
        return Status::OK();
    }
}

Status ClientImpl::GetNodeID(const mspb::GetNodeIdRequest& req, mspb::GetNodeIdResponse& resp) {
    return SyncCall(req, &MsServer::Stub::GetNodeId, kDefaultRPCTimeoutMS, resp);
}

Status ClientImpl::NodeLogin(const mspb::NodeLoginRequest& req, mspb::NodeLoginResponse& resp) {
    return SyncCall(req, &MsServer::Stub::NodeLogin, kDefaultRPCTimeoutMS, resp);
}

Status ClientImpl::GetNodeAddress(const mspb::GetNodeRequest& req, mspb::GetNodeResponse& resp) {
    return SyncCall(req, &MsServer::Stub::GetNode, kDefaultRPCTimeoutMS, resp);
}


template <class RequestT, class ResponseT, class MethodFunc>
Status ClientImpl::AsyncCall(const RequestT& req, AsyncCallType type, MethodFunc func_ptr) {
    auto conn = getLeaderConn();
    if (conn == nullptr) {
        return Status(Status::kNoLeader);
    }

    auto call = new AsyncCallResultT<ResponseT>;
    call->type = type;
    call->response_reader = (conn->GetStub().*func_ptr)(&call->context, req, &cq_);
    call->Finish();
    return Status::OK();
}

Status ClientImpl::AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest& req) {
    return AsyncCall<NodeHeartbeatRequest, NodeHeartbeatResponse>(req,
        AsyncCallType::kNodeHeartbeat, &MsServer::Stub::AsyncNodeHeartbeat);
}

Status ClientImpl::AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest& req) {
    return AsyncCall<NodeHeartbeatRequest, NodeHeartbeatResponse>(req,
        AsyncCallType::kRangeHeartbeat, &MsServer::Stub::AsyncRangeHeartbeat);
}

Status ClientImpl::AsyncAskSplit(const mspb::AskSplitRequest& req) {
    return AsyncCall<AskSplitRequest, AskSplitResponse>(req,
        AsyncCallType::kAskSplit, &MsServer::Stub::AsyncAskSplit);
}

Status ClientImpl::AsyncReportSplit(const mspb::ReportSplitRequest& req) {
    return AsyncCall<ReportSplitRequest, ReportSplitResponse>(req,
        AsyncCallType::kReportSplit, &MsServer::Stub::AsyncReportSplit);
}

void ClientImpl::set_leader(const std::string& leader) {
    FLOG_INFO("[Master] Leader set to %s.", leader.c_str());

    std::lock_guard<std::mutex> lock(addr_mu_);
    leader_ = leader;
    if (!leader.empty()) {
        addAddr(leader);
    }
}

std::string ClientImpl::leader() const {
    std::lock_guard<std::mutex> lock(addr_mu_);
    return leader_;
}

std::string ClientImpl::candidate() {
    std::lock_guard<std::mutex> lock(addr_mu_);
    assert(!ms_addrs_.empty());
    size_t idx = (magic_counter_++) % ms_addrs_.size();
    return ms_addrs_[idx];
}

void ClientImpl::addAddr(const std::string& addr) {
    assert(!addr.empty());
    auto it = std::find(ms_addrs_.begin(), ms_addrs_.end(), addr);
    if (it == ms_addrs_.end()) {
        ms_addrs_.push_back(addr);
    }
}

void ClientImpl::updateLeader(const std::string& from,
                          const mspb::GetMSLeaderResponse& resp) {
    if (resp.has_leader()) {
        FLOG_INFO("[Master] GetMSLeader resp from %s report new leader=%s.", from.c_str(),
                  resp.leader().address().c_str());

        set_leader(resp.leader().address());
    } else {
        FLOG_WARN("[Master] GetMSLeader resp from %s leader not set.", from.c_str());
    }
}

void ClientImpl::askLeader() {
    mspb::GetMSLeaderRequest req;
    mspb::GetMSLeaderResponse resp;
    grpc::ClientContext context;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(kRpcTimeoutMs);
    context.set_deadline(deadline);

    std::string addr = this->candidate();
    auto conn = getConnection(addr);
    FLOG_DEBUG("[Master] ask leader to addr %s.", addr.c_str());

    auto status = conn->GetStub()->GetMSLeader(&context, req, &resp);
    if (!status.ok()) {
        FLOG_ERROR("[Master] call GetMSLeader to %s failed. code=%d, errmsg=%s.",
                   addr.c_str(), status.error_code(), status.error_message().c_str());
    } else if (!checkResponseError(addr, resp.header())) {
        updateLeader(addr, resp);
    }
}

void ClientImpl::asyncAskLeader() {
    // avoid ask too frequently
    static const int kMinAsyncAskIntervalSec = 3;

    auto now = std::chrono::steady_clock::now();
    auto elapse_secs =
        std::chrono::duration_cast<std::chrono::seconds>(now - last_async_ask_).count();
    if (elapse_secs <= kMinAsyncAskIntervalSec) {
        return;
    } else {
        last_async_ask_ = now;
    }

    std::string addr = this->candidate();
    auto conn = getConnection(addr);
    FLOG_DEBUG("[Master] async ask leader to addr %s.", addr.c_str());

    mspb::GetMSLeaderRequest req;

    auto call = new AsyncCallResultT<mspb::GetMSLeaderResponse>;
    call->type = AsyncCallType::kGetMSLeader;
    call->response_reader = conn->GetStub()->AsyncGetMSLeader(&call->context, req, &cq_);
    call->Finish();
}

ClientImpl::ConnPtr ClientImpl::getConnection(const std::string& addr) {
    assert(!addr.empty());

    std::lock_guard<std::mutex> lock(conn_mu_);
    auto it = connections_.find(addr);
    if (it != connections_.end()) {
        return it->second;
    } else {
        auto conn = std::make_shared<Connection>(addr, cq_);
        connections_.emplace(addr, conn);
        return conn;
    }
}

ClientImpl::ConnPtr ClientImpl::getLeaderConn() {
    auto leader_addr = leader();
    // 没leader先尝试问一次leader
    if (leader_addr.empty()) {
        askLeader();
        leader_addr = leader();
    }
    // 还是没leader就报错
    if (leader_addr.empty()) {
        FLOG_WARN("[Master] no ms leader current time.");
        return nullptr;
    } else {
        return getConnection(leader_addr);
    }
}

void ClientImpl::recvResponse(TaskHandler* handler) {
    void* tag = nullptr;
    bool ok = false;
    while (true) {
        tag = nullptr;
        ok = false;
        cq_.Next(&tag, &ok);
        if (!ok) {
            return;
        }

        AsyncCallResult* call = static_cast<AsyncCallResult*>(tag);
        if (call->type != AsyncCallType::kGetMSLeader) {
            checkStatus(call->status);
        }

        std::string from = call->context.peer();
        if (!call->status.ok()) {
            FLOG_ERROR("[Master] rpc failed to %s. type=%s, code=%d, msg=%s.",
                       from.c_str(), AsyncCallTypeName(call->type).c_str(),
                       call->status.error_code(), call->status.error_message().c_str());
        } else {
            FLOG_DEBUG("[Master] recv %s respone from %s.",
                       AsyncCallTypeName(call->type).c_str(), from.c_str());
            dispatchResponse(handler, from, call);
        }
        delete call;
    }
}

void ClientImpl::dispatchResponse(TaskHandler* handler, const std::string& from,
                              AsyncCallResult* call) {
    switch (call->type) {
        case AsyncCallType::kAskSplit: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::AskSplitResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnAskSplitResp(res->response);
            }
            break;
        }
        case AsyncCallType::kReportSplit: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::ReportSplitResponse>*>(call);
            checkResponseError(from, res->response.header());
            break;
        }
        case AsyncCallType::kNodeHeartbeat: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::NodeHeartbeatResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnNodeHeartbeatResp(res->response);
            }
            break;
        }
        case AsyncCallType::kRangeHeartbeat: {
            auto res =
                dynamic_cast<AsyncCallResultT<mspb::RangeHeartbeatResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                handler->OnRangeHeartbeatResp(res->response);
            }
            break;
        }
        case AsyncCallType::kGetMSLeader: {
            auto res = dynamic_cast<AsyncCallResultT<mspb::GetMSLeaderResponse>*>(call);
            if (!checkResponseError(from, res->response.header())) {
                updateLeader(from, res->response);
            }
            break;
        }
        default:
            FLOG_ERROR("[Master] recv unkown response type=%s from %s.",
                       AsyncCallTypeName(call->type).c_str(), from.c_str());
    }
}

void ClientImpl::checkStatus(const grpc::Status& s) {
    static const uint64_t kAskThreshold = 5;

    if (s.error_code() == grpc::DEADLINE_EXCEEDED ||
        s.error_code() == grpc::UNAVAILABLE) {
        if (++network_fail_count_ == kAskThreshold) {
            asyncAskLeader();
            network_fail_count_ = 0;
        }
    } else if (s.ok()) {
        network_fail_count_ = 0;
    }
}

bool ClientImpl::checkResponseError(const std::string& from,
                                const mspb::ResponseHeader& header) {
    if (header.has_error()) {
        const auto& error = header.error();
        if (error.has_new_leader()) {
            FLOG_INFO("[Master] resp header from %s report new leader=%s.", from.c_str(),
                      error.new_leader().address().c_str());

            set_leader(error.new_leader().address());
        } else if (error.has_no_leader()) {
            FLOG_WARN("[Master] resp header from %s report no leader.", from.c_str());

            set_leader("");
        }
        return true;
    } else {
        return false;
    }
}

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
