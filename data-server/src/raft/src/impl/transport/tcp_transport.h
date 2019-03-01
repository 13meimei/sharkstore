_Pragma("once");

#include "transport.h"
#include "raft/node_resolver.h"
#include "net/server.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class TcpConnection : public Connection {
public:
    TcpConnection();

    Status Send(MessagePtr& msg) override;
    Status Close() override;
};

class TcpTransport : public Transport {
public:
    TcpTransport(const std::shared_ptr<NodeResolver>& resolver,
            size_t send_threads_num, size_t recv_threads_num);

    ~TcpTransport();

    Status Start(const std::string& listen_ip, uint16_t listen_port,
            const MessageHandler& handler) override;

    void Shutdown() override;

    void SendMessage(MessagePtr& msg) override;

    // 需要单独建立一个连接用来发快照
    Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) override;

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);

private:
    std::unique_ptr<net::Server> server_;
    MessageHandler handler_;
};


} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
