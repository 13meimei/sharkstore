#ifndef __SF_SOCKET_SESSION_H__
#define __SF_SOCKET_SESSION_H__

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/socket.h>

#include "sf_socket_buff.h"

#include <fastcommon/fast_task_queue.h>
#include <fastcommon/hash.h>

#include "lk_queue/lk_queue.h"

typedef enum socket_state_s {
    SS_INIT,
    SS_OK,
    SS_CLOSED
} socket_state_t;

typedef struct sf_session_entry_s {
    int64_t session_id;
    pthread_rwlock_t session_lock; //protect is_sending
    bool is_sending;
    bool is_attach;               //whether to add send ioevent
    socket_state_t state;

    struct fast_task_info *rtask; //read task
    struct fast_task_info *stask; //send task

    lock_free_queue_t *send_queue;
    pthread_mutex_t   swap_mutex; // for response->buff swap task->data
} sf_session_entry_t;

typedef struct sf_socket_session_s {
    HashArray session_array;
    pthread_rwlock_t array_lock;

} sf_socket_session_t;

#ifdef __cplusplus
extern "C" {
#endif

int sf_socket_session_init(sf_socket_session_t *session);
void sf_socket_session_destroy(sf_socket_session_t *session);

sf_session_entry_t*
sf_create_socket_session(sf_socket_session_t *session, int64_t session_id);
void sf_session_free(sf_socket_session_t *session, int64_t session_id);

int sf_send_task_push(sf_socket_session_t *session, response_buff_t *send_data);
int sf_send_task_finish(sf_socket_session_t *session, int64_t session_id);

void sf_socket_session_close(sf_socket_session_t *session, struct fast_task_info *task);
bool sf_socket_session_closed(sf_socket_session_t *session, int64_t session_id);

#ifdef __cplusplus
}
#endif

#endif  //__SF_SOCKET_SESSION_H__
