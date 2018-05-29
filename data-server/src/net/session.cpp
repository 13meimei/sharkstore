#include "session.h"

// TODO: log
#include <asio/read.hpp>
#include <asio/read_until.hpp>
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

bool Session::init() {
    try {
        msg_ctx_.local_addr = socket_.local_endpoint().address().to_string();
        msg_ctx_.remote_addr = socket_.remote_endpoint().address().to_string();
    } catch (std::exception& e) {
        std::cerr << "[S] get socket addr error: " << e.what() << std::endl;
        return false;
    }
    msg_ctx_.session = shared_from_this();
    id_ = std::string("[S:") + msg_ctx_.remote_addr + "]";
    return true;
}

void Session::Start() {
    if (init()) {
        readPreface();
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
}

void Session::readPreface() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(preface_.data(), preface_.size()),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             if (ValidRPCMagic(preface_)) {
                                 readRPCHead();
                             } else {
                                 parseCmdLine(0);
                                 readCmdLine();
                             }
                         } else {
                             std::cerr << id_ << " read  perface error:" << ec.message()
                                       << std::endl;
                         }
                     });
}

void Session::readRPCHead() {
    void* buf = &rpc_head_;
    size_t buf_size = sizeof(rpc_head_);
    if (preface_remained_ > 0) {
        buf = static_cast<char*>(buf) + preface_.size();
        buf_size -= preface_.size();
        preface_remained_ = 0;
    }
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(buf, buf_size),
        [this, self](std::error_code ec, std::size_t) {
            if (!ec) {
                rpc_head_.Decode();
                if (rpc_head_.Valid()) {
                    readRPCBody();
                } else {
                    std::cerr << id_ << " invalid rpc head: " << rpc_head_.DebugString()
                              << std::endl;
                }
            } else {
                std::cerr << id_ << " read rpc head error:" << ec.message() << std::endl;
            }
        });
}

void Session::readRPCBody() {
    rpc_body_.resize(rpc_head_.body_length);
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(rpc_body_.data(), rpc_body_.size()),
        [this, self](std::error_code ec, std::size_t) {
            if (!ec) {
                msg_handler_.rpc_handler(msg_ctx_, rpc_head_, std::move(rpc_body_));
                readRPCHead();
            } else {
                std::cerr << id_ << " read rpc body error:" << ec.message() << std::endl;
            }
        });
}

void Session::parseCmdLine(std::size_t length) {
    std::string cmdline;
    if (preface_remained_ > 0) {
        uint8_t ch = static_cast<uint8_t>('\n');
        for (int i = 4 - preface_remained_; i < 4; ++i) {
            if (preface_[i] == ch) {
                preface_remained_ -= (cmdline.size() + 1);
                msg_handler_.telnet_handler(msg_ctx_, cmdline);
                cmdline.clear();
            } else {
                cmdline.push_back(static_cast<char>(*it));
            }
        }
    }
    if (length > 0) {
        preface_remained_ = 0;
        cmdline.append(asio::buffer_cast<const char*>(cmdline_buffer_.data()), length);
        msg_handler_.telnet_handler(msg_ctx_, cmdline);
        cmdline_buffer_.consume(length);
    }
}

void Session::readCmdLine() {
    auto self(shared_from_this());
    asio::async_read_util(
        socket_, cmdline_buffer_, '\n',
        [this, self](std::error_code ec, std::size_t length) {
            if (!ec) {
                readCmdLine();
                parseCmdLine(len);
            } else {
                std::cerr << id_ << " read cmdline error:" << ec.message() << std::endl;
            }
        });
}

} /* net */
} /* dataserver  */
} /* sharkstore  */
