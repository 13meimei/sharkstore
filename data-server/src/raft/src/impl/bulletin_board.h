_Pragma("once");

#include <atomic>
#include <mutex>
#include <vector>
#include "base/shared_mutex.h"
#include "raft.pb.h"
#include "raft/status.h"
#include "raft/types.h"

namespace fbase {
namespace raft {
namespace impl {

class BulletinBoard {
public:
    BulletinBoard() = default;
    ~BulletinBoard() = default;

    BulletinBoard(const BulletinBoard&) = delete;
    BulletinBoard& operator=(const BulletinBoard&) = delete;

    void publish(uint64_t leader, uint64_t term);
    void publish(const std::vector<DownPeer>& downs);
    void publish(const std::vector<uint64_t>& pendings);
    void publish(std::vector<Peer> peers);
    void publish(RaftStatus status);

    uint64_t leader() const { return leader_; }
    uint64_t term() const { return term_; }
    void leaderTerm(uint64_t* leader, uint64_t* term) const;

    void peers(std::vector<Peer>* peers) const;
    void down_peers(std::vector<DownPeer>* downs) const;
    void pending_peers(std::vector<uint64_t>* pendings) const;

    void status(RaftStatus* status) const;

private:
    std::atomic<uint64_t> leader_{0};
    std::atomic<uint64_t> term_{0};

    std::vector<Peer> peers_;
    std::vector<DownPeer> down_peers_;
    std::vector<uint64_t> pending_peers_;

    RaftStatus status_;

    mutable fbase::shared_mutex mu_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
