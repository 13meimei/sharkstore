_Pragma("once");

#include <google/protobuf/message.h>
#include "base/status.h"
#include "server/context_server.h"
#include "storage/db/db_interface.h"
#include "common/ds_config.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class PersistServer final {
public:
    struct Options {
        uint64_t thread_num = 4;
        uint64_t delay_count = 10000;
    };
public:
    explicit PersistServer(const Options& ops);
    ~PersistServer();

    PersistServer(const PersistServer&) = delete;
    PersistServer& operator=(const PersistServer&) = delete;

    int Init(ContextServer *context);
    Status Start();
    Status Stop();

    int OpenDB();
    void CloseDB();
private:
    const Options ops_;
    ContextServer* context_ = nullptr;
    storage::DbInterface* db_ = nullptr;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

