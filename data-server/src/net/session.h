_Pragma("once");

#include <asio/ip/tcp.hpp>
#include <asio/streambuf.hpp>
#include <memory>

#include "handler.h"
#include "options.h"
#include "protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(const SessionOptions& opt, const Handler& msg_handler,
            asio::ip::tcp::socket socket);
    ~Session();

    void Start();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    static uint64_t TotalCount() { return total_count_; }

private:
    // all server's sessions count
    static std::atomic<uint64_t> total_count_;

private:
    bool init();
    void doClose();

    void readHead();
    void readBody();

private:
    const SessionOptions& opt_;
    const Handler& handler_;

    asio::ip::tcp::socket socket_;
    Context session_ctx_;
    std::string id_;

    std::atomic<bool> closed_ = {false};

    Head head_;
    std::vector<uint8_t> body_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
