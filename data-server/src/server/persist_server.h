_Pragma("once");

#include <google/protobuf/message.h>
#include "base/status.h"
#include "server/context_server.h"
#include "storage/db/db_interface.h"
#include "common/ds_config.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "raft/raft_log_reader.h"
#include "proto/gen/metapb.pb.h"
#include "storage/meta_store.h"
#include "common_thread.h"

namespace sharkstore {
namespace dataserver {
    class WorkThread;
namespace server {

using WorkThread = sharkstore::dataserver::WorkThread;
class PersistServer final: public std::enable_shared_from_this<PersistServer> {
public:
    struct Options {
        uint64_t thread_num = 4;
        uint64_t delay_count = 10000;
        uint64_t queue_capacity = 100000;
    };
public:
    PersistServer();
    PersistServer(const Options& ops);
    ~PersistServer();

    PersistServer(const PersistServer&) = delete;
    PersistServer& operator=(const PersistServer&) = delete;

    int Init(ContextServer *context);
    Status Start();
    Status Stop();
    bool PersistServer::IndexInDistance(const uint64_t range_id,
                                        const uint64_t aidx, const uint64_t pidx);

    Status SavePersistIndex(const uint64_t range_id, const uint64_t persist_index);
    Status LoadPersistIndex(const uint64_t range_id, uint64_t* persist_index);

    int OpenDB();
    void CloseDB();

    storage::IteratorInterface* GetIterator(const std::string& start, const std::string& limit);

    Status GetWorkThread(const uint64_t range_id, dataserver::WorkThread*& trd);
    Status CreateReader(const uint64_t range_id,
                        const uint64_t start_index,
                        std::shared_ptr<raft::RaftLogReader>* reader);


private:
    uint64_t pindex_ = 0;
    Options ops_;
    ContextServer* context_ = nullptr;
    storage::MetaStore* meta_ = nullptr;
    storage::DbInterface* pdb_ = nullptr;

    std::atomic<bool> running_ = {false};

    std::unordered_map<uint64_t, std::shared_ptr<raft::RaftLogReader>> readers_;

    std::vector<WorkThread*> threads_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

