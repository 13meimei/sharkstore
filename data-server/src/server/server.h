_Pragma("once");

#include <thread>
#include "context_server.h"

namespace sharkstore {
namespace dataserver {

namespace admin { class AdminServer; }

namespace server {

class RPCServer;

class DataServer {
public:
    ~DataServer();

    DataServer(const DataServer &) = delete;
    DataServer &operator=(const DataServer &) = delete;

    static DataServer &Instance() {
        static DataServer instance_;
        return instance_;
    };

    int Start();
    void Stop();

    ContextServer *context_server() { return context_; }

private:
    DataServer();

    bool startRaftServer();
    void nodeHeartbeat();

private:
    const size_t node_heartbeat_secs_ = 10; // 节点心跳间隔

    ContextServer *context_ = nullptr;
    std::unique_ptr<RPCServer> rpc_server_;
    std::unique_ptr<admin::AdminServer> admin_server_;
    std::thread node_hb_thr_; // 节点心跳，定时向master上报信息
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
