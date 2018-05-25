#ifndef __SF_SOCKET_THREAD_H__
#define __SF_SOCKET_THREAD_H__

#include "sf_socket_buff.h"
#include "sf_socket_session.h"
#include "sf_config.h"
#include "sf_status.h"

typedef void (*sf_accept_done_callback_t)(struct fast_task_info *task);

typedef int (*sf_recv_done_callback_t)(struct fast_task_info *task);

typedef int (*sf_send_done_callback_t)(struct fast_task_info *task);

typedef int (*sf_enable_done_callback_t)(struct fast_task_info *task);

typedef int (*sf_socket_close_callback_t)(struct fast_task_info *task, int err);

typedef void (*recv_done_callback_t)(request_buff_t *task, void *args);
typedef void (*send_done_callback_t) (response_buff_t *task, void *args, int err);

typedef struct sf_socket_thread_s {
    sf_socket_thread_config_t *socket_config;
    sf_socket_status_t *socket_status;

    bool is_client;
    int socket_fd;

    struct nio_thread_data *event_recv_data;
    struct nio_thread_data *event_send_data;

    sf_socket_session_t socket_session;

    //developers don't need to set up
    sf_accept_done_callback_t   accept_done_callback;
    sf_recv_done_callback_t     recv_done_callback;
    sf_send_done_callback_t     send_done_callback;
    sf_socket_close_callback_t  socket_close_callback;

    //socket writeable callback (async connect)
    sf_enable_done_callback_t   enable_done_callback;
    //end

    //developers need to set up deal_task_callback
    recv_done_callback_t        recv_callback;
    send_done_callback_t        send_callback;
    void *user_data;
} sf_socket_thread_t;


typedef struct sf_socket_event_s {
    sf_socket_thread_t *context;
    struct nio_thread_data *thread_data;
} sf_socket_event_t;

typedef struct sf_send_thread_s {
    sf_socket_thread_t *context;
    int thread_id;
} sf_send_thread_t;

#ifdef __cplusplus
extern "C" {
#endif

int sf_socket_thread_init(sf_socket_thread_t *context);
void sf_socket_thread_destroy(sf_socket_thread_t *context);

int64_t sf_connect_session_get(sf_socket_thread_t *context, const char *ip_addr, const uint16_t port);
static inline int64_t sf_ip2int64(const char *ip) {return htonl(inet_addr(ip));}

static inline int64_t sf_get_session(const char *ip, const int port) {
    return sf_ip2int64(ip) * 100000 + port;
}

#ifdef __cplusplus
}
#endif


#endif//__SF_SOCKET_THREAD_H__
