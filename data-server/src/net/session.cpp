#include "session.h"

// TODO: log
#include <asio/read.hpp>
#include <iostream>
#include "msg_handler.h"

namespace sharkstore {
namespace dataserver {
namespace net {

std::atomic<uint64_t> Session::total_count_ = {0};

Session::Session(const SessionOptions& opt, const MsgHandler& msg_handler,
                 asio::ip::tcp::socket socket)
    : opt_(opt), msg_handler_(msg_handler), socket_(std::move(socket)) {}

Session::~Session() {
    doClose();

    --total_count_;
}

void Session::Start() {
    try {
        local_addr_ = socket_.local_endpoint().address().to_string();
        remote_addr_ = socket_.remote_endpoint().address().to_string();
    } catch (std::exception& e) {
        std::cout << "[S] get socket addr error: " << e.what() << std::endl;
        doClose();
        return;
    }
    id_ = std::string("[S:") + remote_addr_ + "]";
    readPreface();
}

void Session::doClose() {
    if (closed_) {
        return;
    }

    closed_ = true;
    asio::error_code ec;
    socket_.close(ec);
    (void)ec;
}

void Session::readPreface() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(preface_.data(), preface_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             if (ValidRPCMagic(preface_)) {
                                 readRPCHead();
                             } else {
                                 readCmdLine();
                             }
                         } else {
                             std::cout << id_ << " read  perface error:" << ec.message()
                                       << std::endl;
                         }
                     });
}

void Session::readRPCHead() {
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(&rpc_head_, sizeof(rpc_head_)),
        [this, self](std::error_code ec, std::size_t) {
            if (!ec) {
                rpc_head_.Decode();
                if (rpc_head_.Valid()) {
                    readRPCBody();
                } else {
                    std::cout << id_ << " invalid rpc head: " << rpc_head_.DebugString()
                              << std::endl;
                }
            } else {
                std::cout << id_ << " read rpc head error:" << ec.message() << std::endl;
            }
        });
}

void Session::readRPCBody() {
    // TODO:
}

} /* net */
} /* dataserver  */
} /* sharkstore  */
