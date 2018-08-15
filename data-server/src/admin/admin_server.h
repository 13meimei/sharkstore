_Pragma("once");

#include "server/context_server.h"
#include "net/server.h"

namespace sharkstore {
namespace dataserver {
namespace admin {

class AdminServer {
public:
    explicit AdminServer(server::ContextServer* context);
    ~AdminServer();

    Status Start(uint16_t port);
    Status Stop();

    AdminServer(const AdminServer&) = delete;
    AdminServer& operator=(const AdminServer&) = delete;

private:
    void OnMessage(const net::Context& ctx, const net::MessagePtr& msg);

private:
    server::ContextServer* context_ = nullptr;
    std::unique_ptr<net::Server> net_server_;
};

} // namespace admin
} // namespace dataserver
} // namespace sharkstore
