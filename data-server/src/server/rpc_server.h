_Pragma("once");

#include <google/protobuf/message.h>
#include "base/status.h"
#include "net/server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class Worker;

class RPCServer final {
public:
    explicit RPCServer(const net::ServerOptions& ops);
    ~RPCServer();

    RPCServer(const RPCServer&) = delete;
    RPCServer& operator=(const RPCServer&) = delete;

    Status Start(const std::string& ip, uint16_t port, Worker* worker);
    Status Stop();

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);
    void insert(const net::Context& ctx, const net::MessagePtr& msg);
    void select(const net::Context& ctx, const net::MessagePtr& msg);

    void reply(const net::Context& ctx, const net::Head& req_head,
            const ::google::protobuf::Message& resp);

private:
    const net::ServerOptions ops_;
    std::unique_ptr<net::Server> net_server_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

