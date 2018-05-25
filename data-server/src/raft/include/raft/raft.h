_Pragma("once");

#include "options.h"
#include "status.h"

namespace fbase {
namespace raft {

class Raft {
public:
    Raft() {}
    virtual ~Raft() {}

    Raft(const Raft&) = delete;
    Raft& operator=(const Raft&) = delete;

    virtual bool IsStopped() = 0;

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term) = 0;
    virtual bool IsLeader() = 0;
    virtual Status TryToLeader() = 0;

    virtual Status Submit(std::string& cmd) = 0;
    virtual Status ChangeMemeber(const ConfChange& conf) = 0;

    virtual void GetPeers(std::vector<Peer>* peers) = 0;
    virtual void GetDownPeers(std::vector<DownPeer>* peers) = 0;
    virtual void GetPeedingPeers(std::vector<uint64_t>* peers) = 0;
    virtual void GetStatus(RaftStatus* status) = 0;

    virtual void Truncate(uint64_t index) = 0;
};

} /* namespace raft */
} /* namespace fbase */
