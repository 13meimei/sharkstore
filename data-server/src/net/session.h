_Pragma("once");

#include <queue>
#include <memory>
#include "asio/ip/tcp.hpp"
#include "asio/streambuf.hpp"

#include "message.h"
#include "options.h"
#include "protocol.h"

namespace sharkstore {
namespace net {

class Session : public std::enable_shared_from_this<Session> {
public:
    enum class Direction {
        kClient,
        kServer,
    };

public:
    Session(const SessionOptions& opt, const Handler& handler, asio::ip::tcp::socket socket);
    Session(const SessionOptions& opt, const Handler& handler, asio::io_context& io_context);

    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // for server session
    void Start();

    // for client session
    void Connect(const std::string& address, const std::string& port);

    void Write(const MessagePtr& msg);
    void Close();

private:
    void do_close();
    void do_write();
    bool init_establish();

    void read_head();
    void read_body();

private:
    const SessionOptions& opt_;
    const Handler& handler_;

    asio::ip::tcp::socket socket_;
    Direction direction_;
    std::string local_addr_;
    std::string remote_addr_;
    std::string id_;

    bool established_ = false;
    std::atomic<bool> closed_ = {false};

    Head head_;
    std::vector<uint8_t> body_;

    std::deque<MessagePtr> write_msgs_;
};

}  // namespace net
}  // namespace sharkstore
