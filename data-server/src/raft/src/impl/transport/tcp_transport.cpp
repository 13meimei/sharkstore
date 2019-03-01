#include "tcp_transport.h"

#include <cinttypes>
#include "../logger.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

Status TcpConnection::Send(MessagePtr& msg) {
}

Status TcpConnection::Close() {
}

TcpTransport::TcpTransport(const std::shared_ptr<NodeResolver>& resolver,
             size_t send_threads_num, size_t recv_threads_num) {
    net::ServerOptions sopt;
    sopt.io_threads_num = recv_threads_num;
    server_.reset(new net::Server(sopt, "raft-srv"));
}

TcpTransport::~TcpTransport() {
    Shutdown();
}

void TcpTransport::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    MessagePtr raft_msg(new pb::Message);
    auto data = msg->body.data();
    auto len = static_cast<int>(msg->body.size());
    if (raft_msg->ParseFromArray(data, len)) {
        LOG_DEBUG("recv %s message from %" PRIu64 ":%s to %" PRIu64 ,
                pb::MessageType_Name(raft_msg->type()).c_str(), raft_msg->from(),
                ctx.remote_addr.c_str(), raft_msg->to());

        handler_(raft_msg);
    } else {
        LOG_ERROR("parse raft message failed from %s", ctx.remote_addr.c_str());
    }
}

Status TcpTransport::Start(const std::string& listen_ip, uint16_t listen_port, const MessageHandler& handler) {
    handler_ = handler;
    auto ret = server_->ListenAndServe(listen_ip, listen_port,
                                   [this](const net::Context& ctx, const net::MessagePtr& msg) {
                                       onMessage(ctx, msg);
                                   });
    if (!ret.ok()) {
        return ret;
    }
    return Status::OK();
}

void TcpTransport::Shutdown() {
    if (server_) {
        server_->Stop();
        server_.reset();
    }
}

void TcpTransport::SendMessage(MessagePtr& msg) {
}

Status TcpTransport::GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) {
}


} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
