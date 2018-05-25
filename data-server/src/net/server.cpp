#include "server.h"

#include "handler.h"
#include "io_context_pool.h"
#include "server_connection.h"

namespace fbase {
namespace dataserver {
namespace net {

Server::Server(const ServerOptions& opt)
    : opt_(opt),
      acceptor_(context_),
      context_pool_(new IOContextPool(opt.io_threads_num)) {}

Server::~Server() { Stop(); }

Status Server::ListenAndServe(const std::string& listen_ip,
                              uint16_t listen_port, Handler* handler) {
    std::string bind_ip = listen_ip;
    if (bind_ip.empty()) {
        bind_ip = "0.0.0.0";
    }
    try {
        asio::ip::tcp::endpoint endpoint(asio::ip::make_address(bind_ip),
                                         listen_port);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
    } catch (std::exception& e) {
        return Status(Status::kIOError, "listen", e.what());
    }

    handler_ = handler;
    context_pool_->Start();

    doAccept();
    thr_.reset(new std::thread([this]() {
        try {
            context_.run();
        } catch (...) {
        }
    }));
    thr_->detach();

    return Status::OK();
}

void Server::Stop() {
    if (stopped_) return;

    stopped_ = true;

    acceptor_.close();
    context_.stop();
    context_pool_->Stop();
}

void Server::doAccept() {
    // prepare a new connection for accept
    auto copt = static_cast<ConnectionOptions>(opt_);
    auto connection = std::make_shared<ServerConnection>(copt, getContext());

    // acceptor_.async_accept(connection->GetSocket(), );
}

asio::io_context& Server::getContext() {
    if (context_pool_->Size() > 0) {
        return context_pool_->GetIOContext();
    } else {
        return context_;
    }
}

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
