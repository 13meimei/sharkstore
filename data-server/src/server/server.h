_Pragma("once");

#include "context_server.h"

namespace sharkstore {
namespace dataserver {

namespace admin { class AdminServer; }

namespace server {

class DataServer {
public:
    ~DataServer();

    DataServer(const DataServer &) = delete;
    DataServer &operator=(const DataServer &) = delete;

    static DataServer &Instance() {
        static DataServer instance_;
        return instance_;
    };

    int Init();
    int Start();
    void Stop();

    ContextServer *context_server() { return context_; }

private:
    DataServer();

    bool startRaftServer();

private:
    ContextServer *context_ = nullptr;
    std::unique_ptr<admin::AdminServer> admin_server_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
