_Pragma("once");

#include "server/persist_server.h"

#include <google/protobuf/message.h>
#include "base/status.h"
#include "server/context_server.h"
#include "storage/db/db_interface.h"
#include "proto/gen/metapb.pb.h"
#include "storage/meta_store.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class PersistServerImpl : public PersistServer {
public:
    explicit PersistServerImpl(const PersistOptions& ops);
    ~PersistServerImpl();

    PersistServerImpl(const PersistServerImpl&) = delete;
    PersistServerImpl& operator=(const PersistServerImpl&) = delete;

    int Init(ContextServer *context);
    Status Start();
    Status Stop();

    bool IndexInDistance(const uint64_t range_id, const uint64_t apply_id, const uint64_t persist_id);

    storage::IteratorInterface* GetIterator(const std::string& start, const std::string& limit);

    Status GetWorkThread(const uint64_t range_id, WorkThread*& trd);
    Status CreateReader(const uint64_t range_id,
                        const uint64_t start_index,
                        std::shared_ptr<raft::RaftLogReader>* reader);

    // add delete reader

private:
    
    int OpenDB();
    void CloseDB();

private:
    PersistOptions ops_;
    ContextServer* context_ = nullptr;
    storage::DbInterface* pdb_ = nullptr;
    std::atomic<bool> running_ = {false};

    std::unordered_map<uint64_t, std::shared_ptr<raft::RaftLogReader>> readers_;

    std::vector<WorkThread*> threads_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

