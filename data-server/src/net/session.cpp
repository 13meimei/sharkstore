#include "session.h"

#include "asio/read.hpp"
#include "asio/write.hpp"
#include "asio/read_until.hpp"
#include "asio/connect.hpp"

#include "frame/sf_logger.h"

namespace sharkstore {
namespace net {

Session::Session(const SessionOptions& opt, const Handler& handler, asio::ip::tcp::socket socket) :
    opt_(opt),
    handler_(handler),
    socket_(std::move(socket)) {
    if (opt_.statistics) {
        opt_.statistics->AddSessionCount(1);
    }
}

Session::Session(const SessionOptions& opt, const Handler& handler, asio::io_context& io_context) :
    Session(opt, handler, asio::ip::tcp::socket(io_context)) {
}

Session::~Session() {
    do_close();

    if (opt_.statistics) {
        opt_.statistics->AddSessionCount(-1);
    }

    FLOG_INFO("%s destroyed.", id_.c_str());
}

void Session::Start() {
    direction_ = Direction::kServer;

    if (init_establish()) {
        read_head();
    } else {
        do_close();
    }
}

void Session::Connect(const std::string& address, const std::string& port) {
    direction_ = Direction::kClient;
    id_ = std::string("C[unknown<->") + address + ":" + port + "]";
    remote_addr_ = address + ":" + port;

    asio::ip::tcp::resolver resolver(socket_.get_io_context());
    asio::error_code ec;
    auto endpoints = resolver.resolve(address, port, ec);
    if (ec) {
        FLOG_ERROR("[Net] resolve %s:%s error: %s", address.c_str(), port.c_str(), ec.message().c_str()) ;
        Close();
        return;
    }

    auto self(shared_from_this());
    asio::async_connect(socket_, endpoints,
                        [self, address, port](asio::error_code ec, asio::ip::tcp::endpoint) {
                            if (ec) {
                                FLOG_ERROR("[Net] connect to %s:%s error: %s", address.c_str(),
                                        port.c_str(), ec.message().c_str());
                                self->do_close();
                                return;
                            }
                            if (!self->init_establish()) {
                                self->do_close();
                                return;
                            }
                            // start read
                            self->read_head();
                            // start send
                            if (!self->write_msgs_.empty()) {
                                self->do_write();
                            }
                        });
}

bool Session::init_establish() {
    try {
        auto local_ep = socket_.local_endpoint();
        local_addr_  = local_ep.address().to_string() + ":" + std::to_string(local_ep.port());

        auto remote_ep = socket_.remote_endpoint();
        remote_addr_ = remote_ep.address().to_string() + ":" + std::to_string(remote_ep.port());
    } catch (std::exception& e) {
        FLOG_ERROR("[Net] get socket addr error: %s", e.what());
        return false;
    }

    id_.clear();
    id_.push_back(direction_ == Direction::kServer ? 'S' : 'C');
    id_ += "[" + local_addr_ + "<->" + remote_addr_ + "]";

    established_ = true;
    FLOG_INFO("%s establised.", id_.c_str());

    return true;
}

void Session::Close() {
    if (closed_) {
        return;
    }
    auto self(shared_from_this());
    asio::post(socket_.get_io_context(), [self] { self->do_close(); });
}

void Session::do_close() {
    if (closed_) {
        return;
    }

    closed_ = true;
    asio::error_code ec;
    socket_.close(ec);
    (void)ec;

    FLOG_INFO("%s closed. ", id_.c_str());
}

void Session::read_head() {
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(&head_, sizeof(head_)),
                     [this, self](std::error_code ec, std::size_t) {
                         if (!ec) {
                             head_.Decode();
                             auto ret = head_.Valid();
                             if (ret.ok()) {
                                 read_body();
                             } else {
                                 FLOG_ERROR("%s invalid rpc head: %s", id_.c_str(), ret.ToString().c_str());
                                 do_close();
                             }
                         } else {
                             if (ec == asio::error::eof) {
                                 FLOG_INFO("%s read rpc head error: %s", id_.c_str(), ec.message().c_str());
                             } else {
                                 FLOG_ERROR("%s read rpc head error: %s", id_.c_str(), ec.message().c_str());
                             }
                             do_close();
                         }
                     });
}

void Session::read_body() {
    if (head_.body_length == 0) {
        if (head_.func_id == kHeartbeatFuncID) { // response heartbeat
            if (opt_.statistics) {
                opt_.statistics->AddMessageRecv(sizeof(head_));
            }
            auto msg = NewMessage();
            msg->head.SetResp(head_, 0);
            Write(msg);
        }
        read_head();
        return;
    }

    body_.resize(head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(body_.data(), body_.size()),
                     [this, self](std::error_code ec, std::size_t length) {
                         if (!ec) {
                             if (opt_.statistics) {
                                 opt_.statistics->AddMessageRecv(sizeof(head_) + length);
                             }

                             Context ctx;
                             ctx.session = self;
                             ctx.local_addr = local_addr_;
                             ctx.remote_addr = remote_addr_;
                             auto msg = NewMessage();
                             msg->head = head_;
                             msg->body = std::move(body_);
                             handler_(ctx, msg);

                             read_head();
                         } else {
                             if (ec == asio::error::eof) {
                                 FLOG_INFO("%s read rpc body error: %s", id_.c_str(),
                                            ec.message().c_str());
                             } else {
                                 FLOG_ERROR("%s read rpc body error: %s", id_.c_str(),
                                            ec.message().c_str());
                             }
                             do_close();
                         }
                     });
}

void Session::do_write() {
    auto self(shared_from_this());

    // prepare write buffer
    auto msg = write_msgs_.front();
    msg->head.body_length = static_cast<uint32_t>(msg->body.size());
    msg->head.Encode();
    std::vector<asio::const_buffer> buffers{
        asio::buffer(&msg->head, sizeof(msg->head)),
        asio::buffer(msg->body.data(), msg->body.size())
    };

    asio::async_write(socket_, buffers,
                      [this, self](std::error_code ec, std::size_t length) {
                          if (!ec) {
                              if (opt_.statistics) {
                                  opt_.statistics->AddMessageSent(length);
                              }
                              write_msgs_.pop_front();
                              if (!write_msgs_.empty()) {
                                  do_write();
                              }
                          } else {
                              FLOG_ERROR("%s write message error: %s", id_.c_str(), ec.message().c_str());
                              do_close();
                          }
                      });
}

void Session::Write(const MessagePtr& msg) {
    if (closed_) {
        return;
    }

    auto self(shared_from_this());
    asio::post(socket_.get_io_context(), [self, msg] {
        bool write_in_progress = !self->write_msgs_.empty();
        self->write_msgs_.push_back(msg);
        if (!write_in_progress && self->established_) {
            self->do_write();
        }
    });
}

} /* net */
} /* sharkstore  */
