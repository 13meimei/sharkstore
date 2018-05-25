#include "bulletin_board.h"

namespace fbase {
namespace raft {
namespace impl {

using fbase::shared_lock;
using fbase::shared_mutex;
using std::unique_lock;

void BulletinBoard::publish(uint64_t leader, uint64_t term) {
    unique_lock<shared_mutex> lock(mu_);
    leader_ = leader;
    term_ = term;
}

void BulletinBoard::publish(const std::vector<DownPeer>& downs) {
    unique_lock<shared_mutex> lock(mu_);
    down_peers_ = downs;
}

void BulletinBoard::publish(const std::vector<uint64_t>& pendings) {
    unique_lock<shared_mutex> lock(mu_);
    pending_peers_ = pendings;
}

void BulletinBoard::publish(std::vector<Peer> peers) {
    unique_lock<shared_mutex> lock(mu_);
    peers_ = std::move(peers);
}

void BulletinBoard::publish(RaftStatus status) {
    unique_lock<shared_mutex> lock(mu_);
    status_ = std::move(status);
}

void BulletinBoard::leaderTerm(uint64_t* leader, uint64_t* term) const {
    shared_lock<shared_mutex> lock(mu_);
    *leader = leader_;
    *term = term_;
}

void BulletinBoard::peers(std::vector<Peer>* peers) const {
    shared_lock<shared_mutex> lock(mu_);
    peers->assign(peers_.cbegin(), peers_.cend());
}

void BulletinBoard::down_peers(std::vector<DownPeer>* downs) const {
    shared_lock<shared_mutex> lock(mu_);
    downs->assign(down_peers_.cbegin(), down_peers_.cend());
}

void BulletinBoard::pending_peers(std::vector<uint64_t>* pendings) const {
    shared_lock<shared_mutex> lock(mu_);
    pendings->assign(pending_peers_.cbegin(), pending_peers_.cend());
}

void BulletinBoard::status(RaftStatus* status) const {
    shared_lock<shared_mutex> lock(mu_);
    *status = status_;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
