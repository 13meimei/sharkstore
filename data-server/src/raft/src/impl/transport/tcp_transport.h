_Pragma("once");

#include "transport.h"
#include "base/shared_mutex.h"
#include "net/server.h"
#include "net/context_pool.h"
#include "net/session.h"
#include "raft/options.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace transport {

class TcpConnection : public Connection {
public:
    explicit TcpConnection(const std::shared_ptr<net::Session>& session);

    Status Send(MessagePtr& msg) override;
    Status Close() override;

private:
    std::weak_ptr<net::Session> session_;
};

using TcpConnPtr = std::shared_ptr<TcpConnection>;


class TcpTransport : public Transport {
public:
    explicit TcpTransport(const TransportOptions& ops);

    ~TcpTransport();

    Status Start(const std::string& listen_ip, uint16_t listen_port,
            const MessageHandler& handler) override;

    void Shutdown() override;

    void SendMessage(MessagePtr& msg) override;

    // 需要单独建立一个连接用来发快照
    Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) override;

private:
    using CreateConnFunc = std::function<Status(uint64_t, TcpConnPtr&)>;

    // Group管理到不同NodeID的连接，每个NodeID一个连接
    class ConnectionGroup {
    public:
        explicit ConnectionGroup(const CreateConnFunc& create_func);

        ConnectionGroup(const ConnectionGroup&) = delete;
        ConnectionGroup& operator=(const ConnectionGroup&) = delete;

        TcpConnPtr Get(uint64_t to);
        void Remove(uint64_t to, const TcpConnPtr& conn);

    private:
        CreateConnFunc create_func_;
        std::unordered_map<uint64_t, TcpConnPtr> connections_;
        mutable sharkstore::shared_mutex mu_;
    };

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);
    Status newConnection(uint64_t to, TcpConnPtr& conn);

private:
    std::shared_ptr<NodeResolver> resolver_;

    std::unique_ptr<net::Server> server_;
    MessageHandler handler_;

    std::vector<std::unique_ptr<ConnectionGroup>> conn_pool_;
    std::unique_ptr<net::IOContextPool> client_; // 客户端IO线程
    net::SessionOptions client_opt_;
};


} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
