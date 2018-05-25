_Pragma("once");

#include <chrono>
#include <memory>
#include <vector>

#include "raft/node_resolver.h"
#include "raft/statemachine.h"

namespace fbase {
namespace raft {

struct RaftServerOptions {
    // 本实例dataserver的节点id
    uint64_t node_id = 0;

    // 网络传输, 测试
    bool use_inprocess_transport = false;

    // raft通信模块监听端口
    uint16_t listen_port = 0;

    // resolver获取其他dataserver地址
    std::shared_ptr<NodeResolver> resolver;

    // 是否启用raft pre-vote特性
    bool enable_pre_vote = true;

    // tick间隔时间
    std::chrono::milliseconds tick_interval = std::chrono::milliseconds(500);

    // 选举超时，单位：几个心跳间隔
    unsigned election_tick = 5;

    unsigned heartbeat_tick = 1;

    int max_inflight_msgs = 128;

    uint64_t max_size_per_msg = 1024 * 1024;

    // 同时最多允许多个快照在发送中
    uint8_t max_snapshot_concurrency = 5;

    uint8_t consensus_threads_num = 4;
    int consensus_queue_capacity = 100000;

    uint8_t apply_threads_num = 4;
    int apply_queue_capacity = 100000;

    uint8_t transport_send_threads = 4;
    uint8_t transport_recv_threads = 4;

    Status Validate() const;
};

struct RaftOptions {
    // raft group id
    uint64_t id = 0;

    // 成员
    std::vector<Peer> peers;

    // 状态机接口
    std::shared_ptr<StateMachine> statemachine;

    // raft日志不持久化，只在内存中存储, 供测试用
    bool use_memory_storage = false;

    // raft日志存储目录
    std::string storage_path;
    // 单个日志文件的大小
    size_t log_file_size = 1024 * 1024 * 16;
    // 最多保留多少个日志文件，超过就截断旧数据
    size_t max_log_files = 5;
    // 启动时检测到日志损坏是否运行继续启动
    bool allow_log_corrupt = false;

    // 已应用的位置，用于raft启动时recover
    uint64_t applied = 0;

    // 指定leader和leader对应的term
    uint64_t leader = 0;
    uint64_t term = 0;

    Status Validate() const;
};

} /* namespace raft */
} /* namespace fbase */
