#ifndef __SF_STATUS_H__
#define __SF_STATUS_H__

typedef struct sf_socket_status_s {

    int assigned_accept_threads;
    int actual_accept_threads;

    int assigned_event_recv_threads;
    int actual_event_recv_threads;

    int assigned_event_send_threads;
    int actual_event_send_threads;

    int assigned_worker_threads;
    int actual_worker_threads;

    volatile uint64_t current_connections;
    volatile uint64_t current_send_queue_size;
    volatile uint64_t current_recv_pkg_count;

    volatile uint64_t recv_pkg_count;

    volatile uint64_t recv_error_count;
    volatile uint64_t recv_timeout_count;

    volatile uint64_t send_error_count;
    volatile uint64_t send_timeout_count;

    volatile uint64_t connect_error_count;
    volatile uint64_t connect_timeout_count;

    volatile uint64_t big_len_pkg_count;
} sf_socket_status_t;

typedef struct sf_free_buff_status_s {
    int block_size;             //the size of every entry
    int init_count;             //total count of every block
    int max_count;              //max count
    int mem_block_count;        //total count of mem block
    int mem_block_offset;       //current offset of mem_block_count
    int free_count;             //free count of all entry
    int allocated_count;        //allocated count; init_count * mem_block_offset
} sf_free_buff_status_t;

typedef struct sf_status_s {

} sf_status_t;
#endif//__SF_STATUS_H__
