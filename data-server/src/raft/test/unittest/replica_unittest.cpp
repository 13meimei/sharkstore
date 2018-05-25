#include <gtest/gtest.h>

#include "base/util.h"
//#include "raft/status.h"
#include "raft/src/impl/replica.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using fbase::randomInt;
using namespace fbase::raft::impl;

static pb::Peer randPeer() {
    pb::Peer p;
    p.set_type(randomInt() % 2 == 0 ? pb::PEER_NORMAL : pb::PEER_LEARNER);
    p.set_node_id(randomInt());
    return p;
}

// TEST(Replica, Status) {
//    fbase::raft::RaftStatus s;
//    s.node_id = 1;
//    s.leader = 2;
//    s.term = 3;
//    s.index = 4;
//    s.commit = 5;
//    s.applied = 6;
//    s.state = "leader";
//    for (int i = 2; i < 4; ++i) {
//        fbase::raft::ReplicaStatus rs;
//        rs.peer_id = i * 3;
//        rs.match = i * 4;
//        rs.commit = i * 5;
//        rs.next = i * 6;
//        rs.inactive = i * 7;
//        rs.state = "snapshot";
//        s.replicas.emplace(i, rs);
//    }
//    std::cout << s.ToString() << std::endl;
//}

TEST(Replica, SetGet) {
    pb::Peer p = randPeer();
    int max_inflight = randomInt() % 100 + 100;
    Replica r(p, max_inflight);

    // peer
    ASSERT_EQ(r.peer().node_id(), p.node_id());
    ASSERT_EQ(r.peer().type(), p.type());
    p = randPeer();
    r.set_peer(p);
    ASSERT_EQ(r.peer().node_id(), p.node_id());
    ASSERT_EQ(r.peer().type(), p.type());

    // next
    ASSERT_EQ(r.next(), 0);
    uint64_t next = randomInt();
    r.set_next(next);
    ASSERT_EQ(r.next(), next);

    // match
    ASSERT_EQ(r.match(), 0);
    uint64_t match = randomInt();
    r.set_match(match);
    ASSERT_EQ(r.match(), match);

    // commited
    ASSERT_EQ(r.committed(), 0);
    uint64_t commit = randomInt();
    r.set_committed(commit);
    ASSERT_EQ(r.committed(), commit);

    // active
    ASSERT_EQ(r.active(), false);
    r.set_active();
    ASSERT_EQ(r.active(), true);
    sleep(1);
    ASSERT_EQ(r.inactive_seconds(), 1);

    // pending
    ASSERT_EQ(r.pending(), false);
    r.set_pending(true);
    ASSERT_EQ(r.pending(), true);
    r.set_pending(false);
    ASSERT_EQ(r.pending(), false);

    // state
    ASSERT_EQ(r.state(), ReplicaState::kProbe);
    r.becomeSnapshot(123);
    ASSERT_EQ(r.state(), ReplicaState::kSnapshot);
    r.becomeProbe();
    ASSERT_EQ(r.state(), ReplicaState::kProbe);
    r.becomeReplicate();
    ASSERT_EQ(r.state(), ReplicaState::kReplicate);
    r.resetState(ReplicaState::kProbe);
    ASSERT_EQ(r.state(), ReplicaState::kProbe);

    // pause
    ASSERT_FALSE(r.isPaused());
    r.pause();
    ASSERT_TRUE(r.isPaused());
    r.resume();
    ASSERT_FALSE(r.isPaused());
}

TEST(Replica, Update) {
    Replica r(randPeer(), 100);
    r.update(100);
    ASSERT_EQ(r.next(), 101);

    r.pause();
    ASSERT_TRUE(r.maybeUpdate(200, 100));
    ASSERT_EQ(r.match(), 200);
    ASSERT_EQ(r.next(), 201);
    ASSERT_FALSE(r.isPaused());
    ASSERT_EQ(r.committed(), 100);

    ASSERT_FALSE(r.maybeUpdate(200, 101));
    ASSERT_EQ(r.committed(), 101);

    r.becomeReplicate();
    ASSERT_TRUE(r.maybeDecrTo(201, 201, 102));
    ASSERT_EQ(r.next(), 201);
    ASSERT_EQ(r.committed(), 102);
}

TEST(Replica, Inflight) {
    Replica replica(randPeer(), 100);
    auto& inflight = replica.inflight();
    for (int i = 100; i < 200; ++i) {
        inflight.add(i);
        if (i < 199) {
            ASSERT_FALSE(inflight.full());
        }
    }
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(99);
    ASSERT_TRUE(inflight.full());
    inflight.freeFirstOne();  // free 100
    ASSERT_FALSE(inflight.full());
    inflight.add(200);
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(100);  // 100 is already free
    ASSERT_TRUE(inflight.full());
    inflight.freeTo(101);  // free 101
    ASSERT_FALSE(inflight.full());
    inflight.add(201);

    // test reset
    inflight.reset();
    for (int i = 300; i < 400; ++i) {
        inflight.add(i);
        if (i < 399) {
            ASSERT_FALSE(inflight.full());
        }
    }
    ASSERT_TRUE(inflight.full());
}

}  // namespace
