#include "sf_socket_buff.h"

#include <errno.h>
#include <string.h>

#include <fastcommon/pthread_func.h>

#include "sf_logger.h"
static size_t sf_message_size = sizeof(sf_message_t);

response_buff_t *new_response_buff(int buff_size) {
    response_buff_t *response = malloc(sf_message_size);

    if (buff_size > 0) {
        response->buff = malloc(buff_size);
        if (response->buff == NULL) {
            FLOG_ERROR("malloc %d bytes fail, "
                       "errno: %d, error info: %s",
                       buff_size, errno, STRERROR(errno));

            free(response);
            return NULL;
        }
    }

    response->session_id = -1;
    response->buff_len = 0;
    response->begin_time = 0;
    response->expire_time = 0;

    return response;
}

void delete_response_buff(response_buff_t *response) {
    free(response->buff);
    free(response);
}

