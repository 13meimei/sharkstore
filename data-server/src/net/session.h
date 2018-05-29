_Pragma("once");

#include <asio/ip/tcp.hpp>
#include <memory>

#include "options.h"
#include "rpc_protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class MsgHandler;

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(const SessionOptions& opt, const MsgHandler& msg_handler,
            asio::ip::tcp::socket socket);
    ~Session();

    void Start();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    asio::ip::tcp::socket& GetSocket() { return socket_; }

    static uint64_t TotalCount() { return total_count_; }

private:
    // all server's sessions count
    static std::atomic<uint64_t> total_count_;

private:
    void doClose();

    void readPreface();
    void readRPCHead();
    void readRPCBody();
    void readCmdLine();

private:
    const SessionOptions& opt_;
    const MsgHandler& msg_handler_;

    asio::ip::tcp::socket socket_;
    std::string local_addr_;
    std::string remote_addr_;
    std::string id_;

    std::atomic<bool> closed_ = {false};

    std::array<uint8_t, 4> preface_ = {{0, 0, 0, 0}};

    RPCHead rpc_head_;
    std::vector<char> rpc_body_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
