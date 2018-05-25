#include "raft_types.h"

#include <sstream>

namespace fbase {
namespace raft {
namespace impl {

std::string FsmStateName(FsmState state) {
    switch (state) {
        case FsmState::kFollower:
            return "follower";
        case FsmState::kCandidate:
            return "candidate";
        case FsmState::kLeader:
            return "leader";
        case FsmState::kPreCandidate:
            return "pre-candidate";
        default:
            return "unknown";
    }
}

std::string ReplicateStateName(ReplicaState state) {
    switch (state) {
        case ReplicaState::kProbe:
            return "probe";
        case ReplicaState::kReplicate:
            return "replicate";
        case ReplicaState::kSnapshot:
            return "snapshot";
        default:
            return "invalid";
    }
}

pb::Peer toPBPeer(Peer p) {
    pb::Peer pp;
    pp.set_node_id(p.node_id);
    pp.set_peer_id(p.peer_id);
    switch (p.type) {
        case PeerType::kNormal:
            pp.set_type(pb::PEER_NORMAL);
            break;
        case PeerType::kLearner:
            pp.set_type(pb::PEER_LEARNER);
            break;
        default:
            break;
    }
    return pp;
}

Peer fromPBPeer(pb::Peer pp) {
    Peer p;
    p.node_id = pp.node_id();
    p.peer_id = pp.peer_id();
    switch (pp.type()) {
        case pb::PEER_NORMAL:
            p.type = PeerType::kNormal;
            break;
        case pb::PEER_LEARNER:
            p.type = PeerType::kLearner;
            break;
        default:
            break;
    }
    return p;
}

void toPBCC(const ConfChange& cc, pb::ConfChange* pb_cc) {
    switch (cc.type) {
        case ConfChangeType::kAdd:
            pb_cc->set_type(pb::CONF_ADD_NODE);
            break;
        case ConfChangeType::kRemove:
            pb_cc->set_type(pb::CONF_REMOVE_NODE);
            break;
        case ConfChangeType::kUpdate:
            pb_cc->set_type(pb::CONF_UPDATE_NODE);
            break;
        default:
            break;
    }
    pb_cc->mutable_peer()->CopyFrom(toPBPeer(cc.peer));
    pb_cc->set_context(cc.context);
}

void fromPBCC(const pb::ConfChange& pb_cc, ConfChange* cc) {
    switch (pb_cc.type()) {
        case pb::CONF_ADD_NODE:
            cc->type = ConfChangeType::kAdd;
            break;
        case pb::CONF_REMOVE_NODE:
            cc->type = ConfChangeType::kRemove;
            break;
        case pb::CONF_UPDATE_NODE:
            cc->type = ConfChangeType::kUpdate;
            break;
        default:
            break;
    }
    cc->peer = fromPBPeer(pb_cc.peer());
    cc->context = pb_cc.context();
}

std::string MessageToString(const pb::Message& msg) {
    std::ostringstream ss;
    ss << "type=" << pb::MessageType_Name(msg.type()) << ", from=" << msg.from()
       << ", to=" << msg.to() << ", id=" << msg.id() << ", term=" << msg.term();

    return ss.str();
}

bool IsLocalMsg(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_HUP:
        case pb::LOCAL_MSG_PROP:
        case pb::LOCAL_MSG_TICK:
            return true;
        default:
            return false;
    }
}

bool IsResponseMsg(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::APPEND_ENTRIES_RESPONSE:
        case pb::VOTE_RESPONSE:
        case pb::HEARTBEAT_RESPONSE:
        case pb::SNAPSHOT_RESPONSE:
        case pb::SNAPSHOT_ACK:
        case pb::PRE_VOTE_RESPONSE:
            return true;
        default:
            return false;
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
