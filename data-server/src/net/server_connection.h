_Pragma("once");

#include <asio/ip/tcp.hpp>
#include <memory>

#include "options.h"

namespace fbase {
namespace dataserver {
namespace net {

class ServerConnection : public std::enable_shared_from_this<ServerConnection> {
public:
    ServerConnection(const ConnectionOptions& ops, asio::io_context& context);
    ~ServerConnection();

    ServerConnection(const ServerConnection&) = delete;
    ServerConnection& operator=(const ServerConnection&) = delete;

    asio::ip::tcp::socket& GetSocket() { return socket_; }

    static uint64_t GetTotalCount() { return total_count_; }

private:
    // all server's connections count
    static std::atomic<uint64_t> total_count_;

private:
    const ConnectionOptions opt_;
    asio::io_context& io_context_;

    asio::ip::tcp::socket socket_;
    std::array<uint8_t, 8192> buffer_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
