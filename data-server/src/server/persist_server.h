_Pragma("once");

#include <google/protobuf/message.h>
#include "base/status.h"
#include "server/context_server.h"
#include "storage/db/db_interface.h"
#include "common/ds_config.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "raft/storage_reader.h"
#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace raft {
namespace impl {
    class WorkThread;
    namespace storage {
        class StorageReader;
    }
}}}

namespace sharkstore {
namespace dataserver {
namespace server {

using StorageReader = sharkstore::raft::StorageReader;
using WorkThread = sharkstore::raft::impl::WorkThread;

class PersistServer final {
public:
    struct Options {
        uint64_t thread_num = 4;
        uint64_t delay_count = 10000;
        uint64_t queue_capacity = 100000;
    };
public:
    explicit PersistServer(const Options& ops);
    ~PersistServer();

    PersistServer(const PersistServer&) = delete;
    PersistServer& operator=(const PersistServer&) = delete;

    int Init(ContextServer *context);
    Status Start();
    Status Stop();
    void TriggerPersist(const uint64_t range_id, const uint64_t persist, const uint64_t applied);

    int OpenDB();
    void CloseDB();

    Status CreateReader(const uint64_t range_id,
                        const std::function<bool(const std::string&)>& f0,
                        const std::function<bool(const metapb::Range &meta)>& f1,
                        std::shared_ptr<StorageReader>* reader);
private:
    const Options ops_;
    ContextServer* context_ = nullptr;
    storage::DbInterface* db_ = nullptr;

    std::atomic<bool> running_ = {false};

    std::unordered_map<uint64_t, std::shared_ptr<StorageReader>> readers_;

    std::vector<WorkThread*> threads_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

