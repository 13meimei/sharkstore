#ifndef __SF_SOCKET_BUFF_H__
#define __SF_SOCKET_BUFF_H__

#include <stdint.h>
#include <pthread.h>

#include "sf_socket.h"

typedef struct sf_message_s {
    int64_t session_id;
    int64_t msg_id;
    int64_t begin_time;
    int64_t expire_time;
    int32_t buff_len;
    char    *buff;
} sf_message_t;

typedef sf_message_t request_buff_t;
typedef sf_message_t response_buff_t;

#ifdef __cplusplus
extern "C" {
#endif

void delete_response_buff(response_buff_t *buff);
response_buff_t *new_response_buff(int buff_size);

#ifdef __cplusplus
}
#endif

#endif//__SF_SOCKET_BUFF_H__
