#ifndef __SF_SOCKET_H__
#define __SF_SOCKET_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fastcommon/ioevent_loop.h>
#include <fastcommon/fast_task_queue.h>

typedef int (*sf_body_length_callback_t)(struct fast_task_info *task);
typedef int (*sf_socket_timeout_callback_t)(struct fast_task_info *task);

typedef struct {
    void *session;
    void *response;
    void *context;
} sf_task_arg_t;

#ifdef __cplusplus
extern "C" {
#endif

int sf_add_connect_notify(struct fast_task_info *task);
int sf_add_recv_notify(struct fast_task_info *task);
int sf_add_send_notify(struct fast_task_info *task);

int sf_set_connect_event(struct fast_task_info *task);
int sf_set_close_event(struct fast_task_info *task);
int sf_set_recv_event(struct fast_task_info *task);
int sf_set_send_event(struct fast_task_info *task);
void sf_set_send_callback(struct fast_task_info *task);

int sf_clear_recv_event(struct fast_task_info *task);
int sf_clear_send_event(struct fast_task_info *task);

int sf_set_socket_keep(struct fast_task_info *task);
int sf_set_request_timeout(struct fast_task_info *task);

int sf_socket_server(const char *bind_addr, int port, int *sock);
int sf_connect_to_server(const char *host_addr, const uint16_t port, int *sock);
int sf_socket_send_task(struct fast_task_info *task);

void sf_notify_recv(int sock, short event, void *arg);
void sf_notify_send(int sock, short event, void *arg);

void sf_set_body_length_callback(sf_body_length_callback_t body_length_func);
void sf_set_header_size(int size);

#ifdef __cplusplus
}
#endif

#endif  //__SF_SOCKET_H__
