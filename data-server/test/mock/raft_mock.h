#ifndef __RAFT_MOCK_H__
#define __RAFT_MOCK_H__

#include "raft/raft.h"

using namespace fbase;
using namespace fbase::raft;

class RaftMock : public Raft {
public:
    RaftMock(const RaftOptions &ops);

    virtual bool IsStopped() { return false; }

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term);
    virtual bool IsLeader() {return true;}
    virtual Status TryToLeader() {return Status::OK();}

    virtual Status Submit(std::string& cmd);
    virtual Status ChangeMemeber(const ConfChange& conf);

    virtual void GetPeers(std::vector<Peer>* peers) {}
    virtual void GetDownPeers(std::vector<DownPeer>* peers) {}
    virtual void GetPeedingPeers(std::vector<uint64_t>* peers) {}
    virtual void GetStatus(RaftStatus* status) {}

    virtual void Truncate(uint64_t index) {}

private:
    RaftOptions ops_;
};
#endif//__RAFT_MOCK_H__

