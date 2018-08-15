#include "session.h"

#include <asio/read.hpp>
#include <asio/read_until.hpp>

#include "frame/sf_logger.h"

namespace sharkstore {
namespace dataserver {
namespace net {

std::atomic<uint64_t> Session::total_count_ = {0};

Session::Session(const SessionOptions& opt, const Handler & handler,
                 asio::ip::tcp::socket socket)
    : opt_(opt), handler_(handler), socket_(std::move(socket)) {
    ++total_count_;
}

Session::~Session() {
    doClose();

    --total_count_;

    FLOG_DEBUG("%s destroyed.", id_.c_str());
}

bool Session::init() {
    try {
        auto local_ep = socket_.local_endpoint();
        session_ctx_.local_addr =
            local_ep.address().to_string() + ":" + std::to_string(local_ep.port());

        auto remote_ep = socket_.remote_endpoint();
        session_ctx_.remote_addr =
            remote_ep.address().to_string() + ":" + std::to_string(remote_ep.port());
    } catch (std::exception& e) {
        FLOG_ERROR("[S] get socket addr error: %s", e.what());
        return false;
    }

    session_ctx_.session = shared_from_this();
    id_ = std::string("S[") + session_ctx_.remote_addr + "]";

    FLOG_INFO("%s establised %s->%s", id_.c_str(), session_ctx_.remote_addr.c_str(),
              session_ctx_.local_addr.c_str());

    return true;
}

void Session::Start() {
    if (init()) {
        readHead();
    } else {
        doClose();
    }
}

void Session::doClose() {
    if (closed_) {
        return;
    }

    closed_ = true;
    asio::error_code ec;
    socket_.close(ec);
    (void)ec;

    session_ctx_.session.reset();

    FLOG_INFO("%s closed. ", id_.c_str());
}

void Session::readHead() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(&head_, kHeadSize),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             head_.Decode();
                             auto ret = head_.Valid();
                             if (ret.ok()) {
                                 readBody();
                             } else {
                                 FLOG_ERROR("%s invalid rpc head: %s", id_.c_str(), ret.ToString().c_str());
                                 doClose();
                             }
                         } else {
                             FLOG_ERROR("%s read rpc head error: %s", id_.c_str(),
                                        ec.message().c_str());
                             doClose();
                         }
                     });
}

void Session::readBody() {
    body_.resize(head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(body_.data(), body_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             handler_(session_ctx_, head_, std::move(body_));
                             readHead();
                         } else {
                             FLOG_ERROR("%s read rpc body error: %s", id_.c_str(),
                                        ec.message().c_str());
                             doClose();
                         }
                     });
}

} /* net */
} /* dataserver  */
} /* sharkstore  */
