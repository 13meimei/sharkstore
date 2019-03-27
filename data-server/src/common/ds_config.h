#ifndef __DS_CONFIG_H__
#define __DS_CONFIG_H__

#include <limits.h>
#include "frame/sf_config.h"

struct rocksdb_config_t {
    char path[PATH_MAX];
    size_t block_cache_size; // default: 1024MB
    size_t row_cache_size;
    size_t block_size; // default: 16K
    int max_open_files;
    size_t bytes_per_sync;
    size_t write_buffer_size;
    int max_write_buffer_number;
    int min_write_buffer_number_to_merge;
    size_t max_bytes_for_level_base;
    int max_bytes_for_level_multiplier;
    size_t target_file_size_base;
    int target_file_size_multiplier;
    int max_background_flushes;
    int max_background_compactions;
    size_t background_rate_limit;
    bool disable_auto_compactions;
    bool read_checksum;
    int level0_file_num_compaction_trigger;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    bool disable_wal;
    bool cache_index_and_filter_blocks;
    int compression;
    int storage_type;
    int min_blob_size;
    size_t blob_file_size;
    bool enable_garbage_collection;
    int blob_gc_percent;
    int blob_compression;
    size_t blob_cache_size;
    uint64_t blob_ttl_range; // in seconds
    int ttl;
    bool enable_stats;
    bool enable_debug_log;
};


typedef struct ds_config_s {
    int fast_worker_num;  // fast worker thread num; eg. put/get command
    int slow_worker_num;  // fast worker thread num; eg. put/get command
    bool task_in_place; // 任务不走worker，在IO线程中就地执行

    int task_timeout;  // defualt 3,000ms

    struct {
        int node_interval;   // node heartbeat interval
        int range_interval;  // range heartbeat interval
        int master_num;      // master server num
        char **master_host;  // master server host:port
    } hb_config;

    struct rocksdb_config_t rocksdb_config;

    struct {
        bool recover_skip_fail;
        int recover_concurrency;
        uint64_t check_size;
        uint64_t split_size;
        uint64_t max_size;
        int worker_threads;
        int access_mode; // 0 sql, 1 redis, default=0
    } range_config;

    struct {
        int port;  // raft server port
        bool in_memory_log;
        char log_path[PATH_MAX];
        size_t log_file_size;
        size_t max_log_files;
        int allow_log_corrupt;
        size_t consensus_threads;
        size_t consensus_queue;
        size_t apply_threads;
        size_t apply_queue;
        size_t transport_send_threads;
        size_t transport_recv_threads;
        size_t connection_pool_size;
        size_t tick_interval_ms;
        size_t max_msg_size;
    } raft_config;

    struct {
        int interval;
    } metric_config;

    struct {
        int buffer_map_size;
        int buffer_queue_size;
        int watcher_set_size;
        int watcher_thread_priority;
    } watch_config;

    sf_socket_thread_config_t manager_config;  // manager thread config
    sf_socket_thread_config_t worker_config;   // worker thread config

    struct {
        char name[32];
    } engine_config;
} ds_config_t;

extern ds_config_t ds_config;

#ifdef __cplusplus
extern "C" {
#endif

int load_from_conf_file(IniContext *ini_context, const char *filename);

void print_rocksdb_config();
void print_raft_config();

#ifdef __cplusplus
}
#endif

#endif  //__DS_CONFIG_H__
