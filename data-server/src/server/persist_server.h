_Pragma("once");

#include <google/protobuf/message.h>
#include "base/status.h"
#include "server/context_server.h"
#include "raft/raft_log_reader.h"
#include "common_thread.h"

namespace sharkstore {
namespace dataserver {
namespace server {

using WorkThread = sharkstore::dataserver::server::CommonThread;

struct PersistOptions {
    uint64_t thread_num = 4;
    uint64_t delay_count = 10000;
    uint64_t queue_capacity = 100000;
}; 

class PersistServer { 
public:
    PersistServer() = default;
    virtual ~PersistServer() = default;

    PersistServer(const PersistServer&) = delete;
    PersistServer& operator=(const PersistServer&) = delete;

    virtual int Init(ContextServer *context) = 0;
    virtual Status Start() = 0 ;
    virtual Status Stop() = 0 ;
    virtual bool IndexInDistance(const uint64_t range_id, const uint64_t apply_id, const uint64_t persist_id) = 0 ; 
    virtual storage::IteratorInterface* GetIterator(const std::string& start, const std::string& limit) = 0;

    virtual Status GetWorkThread(const uint64_t range_id, WorkThread*& trd) = 0;
    virtual Status CreateReader(const uint64_t range_id, const uint64_t start_index,
                        std::shared_ptr<raft::RaftLogReader>* reader) = 0 ;

};

std::unique_ptr<PersistServer> CreatePersistServer(const PersistOptions & ops);


} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

