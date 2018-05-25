_Pragma("once");

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <thread>

#include "base/status.h"
#include "options.h"

namespace fbase {
namespace dataserver {
namespace net {

class Handler;
class IOContextPool;

class Server final {
public:
    explicit Server(const ServerOptions& opt);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    Status ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                          Handler* handler);
    void Stop();

private:
    void doAccept();
    asio::io_context& getContext();

private:
    const ServerOptions opt_;

    Handler* handler_ = nullptr;

    bool stopped_ = false;

    // acceptor
    asio::io_context context_;
    asio::ip::tcp::acceptor acceptor_;
    std::unique_ptr<std::thread> thr_;

    std::unique_ptr<IOContextPool> context_pool_;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
