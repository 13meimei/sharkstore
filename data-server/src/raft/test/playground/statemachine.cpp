#include "statemachine.h"

#include <iostream>

namespace sharkstore {
namespace raft {
namespace playground {

extern uint64_t gNodeID;

PGStateMachine::PGStateMachine() {}

PGStateMachine::~PGStateMachine() {}

Status PGStateMachine::Apply(const std::string& cmd, uint64_t index) {
    std::cout << "[NODE " << gNodeID << "] apply command: " << cmd << ", index: " << index
              << std::endl;

    sum_ += static_cast<uint64_t>(atoi(cmd.c_str()));
    applied_ = index;

    return Status::OK();
}

static std::string ccTypeName(ConfChangeType type) {
    switch (type) {
        case ConfChangeType::kAdd:
            return "add";
        case ConfChangeType::kRemove:
            return "remove";
        case ConfChangeType::kPromote:
            return "promote";
        default:
            return "unknown";
    }
}

Status PGStateMachine::ApplyMemberChange(const ConfChange& cc, uint64_t index) {
    std::cout << "[NODE " << gNodeID << "] apply member change: " << ccTypeName(cc.type)
              << " " << cc.peer.node_id << ", index: " << index << std::endl;

    return Status::OK();
}

void PGStateMachine::OnReplicateError(const std::string& cmd, const Status& status) {
    std::cout << "[NODE " << gNodeID << "] replicate command: " << cmd
              << " failed: " << status.ToString() << std::endl;
}

void PGStateMachine::OnLeaderChange(uint64_t leader, uint64_t term) {
    std::cout << "[NODE " << gNodeID << "] leader change to " << leader << " at term "
              << term << std::endl;
}

std::shared_ptr<Snapshot> PGStateMachine::GetSnapshot() {
    std::cout << "[NODE " << gNodeID << "] get snapshot" << std::endl;
    return std::shared_ptr<Snapshot>(new PGSnapshot(sum_, applied_));
}

Status PGStateMachine::ApplySnapshotStart(const std::string&) {
    std::cout << "[NODE " << gNodeID << "] start apply snapshot" << std::endl;
    return Status::OK();
}

Status PGStateMachine::ApplySnapshotData(const std::vector<std::string>& datas) {
    for (const auto& data: datas) {
        std::cout << "[NODE " << gNodeID << "] apply snapshot: " << data << std::endl;
        sum_ = static_cast<uint64_t>(atoi(data.c_str()));
    }
    return Status::OK();
}

Status PGStateMachine::ApplySnapshotFinish(uint64_t index) {
    std::cout << "[NODE " << gNodeID << "] apply snapshot finish. index=" << index << std::endl;
    return Status::OK();
}

} /* namespace playground */
} /* namespace raft */
} /* namespace sharkstore */