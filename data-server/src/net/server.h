_Pragma("once");

#include <thread>
#include "asio/io_context.hpp"
#include "asio/ip/tcp.hpp"

#include "base/status.h"

#include "message.h"
#include "options.h"

namespace sharkstore {
namespace net {

class IOContextPool;

class Server final {
public:
    Server(const ServerOptions& opt, const std::string& name);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    Status ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                          const Handler &handler);

    void Stop();

private:
    void doAccept();
    asio::io_context& getContext();

private:
    const std::string name_;
    ServerOptions opt_;
    Handler handler_;

    bool stopped_ = false;

    // acceptor
    asio::io_context context_;
    asio::ip::tcp::acceptor acceptor_;

    std::unique_ptr<IOContextPool> context_pool_;

    std::unique_ptr<std::thread> thr_;
};

}  // namespace net
}  // namespace sharkstore
