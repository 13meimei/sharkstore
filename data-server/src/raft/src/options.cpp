#include "raft/options.h"

namespace fbase {
namespace raft {

Status RaftServerOptions::Validate() const {
    if (node_id == 0) {
        return Status(Status::kInvalidArgument, "raft server options", "node id");
    }
    if (!use_inprocess_transport && listen_port == 0) {
        return Status(Status::kInvalidArgument, "raft server options", "listen port");
    }
    if (consensus_threads_num == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "consensus threads num");
    }
    if (consensus_queue_capacity <= 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "consensus queue capacity");
    }
    if (apply_threads_num == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "apply threads num");
    }
    if (apply_queue_capacity <= 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "apply queue capacity");
    }
    if (transport_send_threads == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "transport send threads");
    }
    if (transport_recv_threads == 0) {
        return Status(Status::kInvalidArgument, "raft server options",
                      "transport recv threads");
    }

    return Status::OK();
}

Status RaftOptions::Validate() const {
    if (id == 0) {
        return Status(Status::kInvalidArgument, "raft options", "id");
    }
    if (peers.empty()) {
        return Status(Status::kInvalidArgument, "raft options", "peers");
    }
    if (!statemachine) {
        return Status(Status::kInvalidArgument, "raft options", "statemachine");
    }
    if (!use_memory_storage) {
        if (storage_path.empty()) {
            return Status(Status::kInvalidArgument, "raft options",
                          "logger storage path");
        }
        if (log_file_size == 0) {
            return Status(Status::kInvalidArgument, "raft options", "log file size");
        }
        if (max_log_files == 0) {
            return Status(Status::kInvalidArgument, "raft options", "max log files");
        }
    }

    // 检查指定的leader是否在成员内
    if (leader != 0) {
        bool found = false;
        for (const auto& p : peers) {
            if (p.node_id == leader) {
                found = true;
                break;
            }
        }
        if (!found) {
            return Status(Status::kInvalidArgument, "raft options",
                          "could not find specified in peers");
        }
    }

    return Status::OK();
}

} /* namespace raft */
} /* namespace fbase */