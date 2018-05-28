#include "bulletin_board.h"

namespace fbase {
namespace raft {
namespace impl {

using fbase::shared_lock;
using fbase::shared_mutex;
using std::unique_lock;

static const int kDownPeerThresholdSecs = 50;

void BulletinBoard::PublishLeaderTerm(uint64_t leader, uint64_t term) {
    unique_lock<shared_mutex> lock(mu_);

    leader_ = leader;
    term_ = term;
}

void BulletinBoard::PublishPeers(std::vector<Peer>&& peers) {
    unique_lock<shared_mutex> lock(mu_);

    peers_ = std::move(peers);
}

void BulletinBoard::PublishStatus(RaftStatus&& status) {
    unique_lock<shared_mutex> lock(mu_);

    status_ = std::move(status);

    down_peers_.clear();
    pending_peers_.clear();
    for (auto& pr : status_.replicas) {
        if (pr.second.inactive >= kDownPeerThresholdSecs) {
            down_peers_.push_back(DownPeer());
            down_peers_.back().peer = pr.second.peer;
            down_peers_.back().down_seconds = pr.second.inactive;
        }
        if (pr.second.pending) {
            pending_peers_.push_back(pr.second.peer);
        }
    }
}

void BulletinBoard::LeaderTerm(uint64_t* leader, uint64_t* term) const {
    shared_lock<shared_mutex> lock(mu_);

    *leader = leader_;
    *term = term_;
}

void BulletinBoard::Peers(std::vector<Peer>* peers) const {
    shared_lock<shared_mutex> lock(mu_);

    *peers = peers_;
}

void BulletinBoard::PendingPeers(std::vector<Peer>* pendings) const {
    shared_lock<shared_mutex> lock(mu_);

    *pendings = pending_peers_;
}

void BulletinBoard::DownPeers(std::vector<DownPeer>* downs) const {
    shared_lock<shared_mutex> lock(mu_);

    *downs = down_peers_;
}

void BulletinBoard::Status(RaftStatus* status) const {
    shared_lock<shared_mutex> lock(mu_);

    *status = status_;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
