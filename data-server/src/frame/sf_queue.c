#include "sf_queue.h"

#include <pthread.h>
#include <string.h>

#include <fastcommon/common_define.h>
#include <fastcommon/pthread_func.h>

#include "sf_logger.h"

static sf_free_queue_t free_queue;

static int sf_mem_block_malloc();

int sf_free_queue_init(int init_count, int max_count) {
    int result;
    if ((result = init_pthread_lock(&free_queue.lock)) != 0) {
        FLOG_ERROR("init_pthread_lock fail, errno: %d, error info: %s", result,
                   STRERROR(result));
        return result;
    }

    free_queue.mem_block_offset = 0;
    free_queue.mem_block_count = max_count / init_count + 1;

    free_queue.block_count = init_count;
    free_queue.block_size = (sizeof(sf_queue_entry_t) + 7) & (~7);

    free_queue.mem_block = malloc(sizeof(void *) * free_queue.mem_block_count);
    if (free_queue.mem_block == NULL) {
        FLOG_ERROR("malloc free_queue %d mem_block_count "
                   "%ld bytes fail, "
                   "errno: %d, error info: %s",
                   free_queue.mem_block_count,
                   sizeof(void *) * free_queue.mem_block_count, errno, STRERROR(errno));
        return ENOMEM;
    }
    return sf_mem_block_malloc();
}

void sf_free_queue_destroy() {
    for (int i = 0; i < free_queue.mem_block_offset; i++) {
        free(free_queue.mem_block[i]);
    }
    free(free_queue.mem_block);
}

sf_queue_entry_t *sf_free_queue_pop() {
    sf_queue_entry_t *entry;
    int result;

    if ((result = pthread_mutex_lock(&free_queue.lock)) != 0) {
        FLOG_ERROR("call pthread_mutex_lock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
        return NULL;
    }

    while (true) {
        entry = free_queue.head;
        if (entry != NULL) {
            free_queue.head = entry->next;
            if (free_queue.head == NULL) {
                free_queue.tail = NULL;
            }
            break;
        }
        if (sf_mem_block_malloc() != 0) {
            break;
        }
    }
    if ((result = pthread_mutex_unlock(&free_queue.lock)) != 0) {
        FLOG_ERROR("call pthread_mutex_unlock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
    }

    return entry;
}

int sf_free_queue_push(sf_queue_entry_t *entry) {
    int result;

    if ((result = pthread_mutex_lock(&free_queue.lock)) != 0) {
        FLOG_ERROR("call pthread_mutex_lock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
        return result;
    }

    entry->data = NULL;
    entry->next = NULL;
    if (free_queue.tail == NULL) {
        free_queue.head = entry;
    } else {
        free_queue.tail->next = entry;
    }
    free_queue.tail = entry;

    if ((result = pthread_mutex_unlock(&free_queue.lock)) != 0) {
        FLOG_ERROR("call pthread_mutex_unlock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
    }

    return 0;
}

inline int sf_queue_init(sf_queue_t *queue) {
    int result;

    pthread_cond_init(&queue->qcond, NULL);

    if ((result = init_pthread_lock(&queue->qmutex)) != 0) {
        FLOG_ERROR("init_pthread_lock fail, errno: %d, error info: %s", result,
                   STRERROR(result));
        return result;
    }

    queue->head = NULL;
    queue->tail = NULL;
    queue->count = 0;

    return 0;
}

inline void *sf_queue_pop(sf_queue_t *queue) {
    sf_queue_entry_t *entry;
    int result;

    if ((result = pthread_mutex_lock(&queue->qmutex)) != 0) {
        FLOG_ERROR("call pthread_mutex_lock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
        return NULL;
    }

    entry = queue->head;
    if (entry != NULL) {
        queue->head = entry->next;
        if (queue->head == NULL) {
            queue->tail = NULL;
        }
        queue->count -= 1;
    }

    if ((result = pthread_mutex_unlock(&queue->qmutex)) != 0) {
        FLOG_ERROR("call pthread_mutex_unlock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
    }

    void *data = entry->data;
    sf_free_queue_push(entry);

    return data;
}

inline int sf_queue_push(sf_queue_t *queue, void *data) {
    int result;

    sf_queue_entry_t *entry = sf_free_queue_pop();
    if (entry == NULL) {
        FLOG_ERROR("no free queue");
        return ENOMEM;
    }

    if ((result = pthread_mutex_lock(&queue->qmutex)) != 0) {
        FLOG_ERROR("call pthread_mutex_lock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
        sf_free_queue_push(entry);
        return result;
    }

    entry->data = data;
    entry->next = NULL;
    if (queue->tail == NULL) {
        queue->head = entry;
    } else {
        queue->tail->next = entry;
    }
    queue->tail = entry;
    queue->count += 1;

    if ((result = pthread_mutex_unlock(&queue->qmutex)) != 0) {
        FLOG_ERROR("call pthread_mutex_unlock fail, "
                   "errno: %d, error info: %s",
                   result, STRERROR(result));
    }
    return 0;
}

void *sf_queue_find_if(sf_queue_t *queue, sf_queue_find_t find_func, void *args) {
    int result;
    void *data;
    sf_queue_entry_t *pre, *cur;

    do {
        if ((result = pthread_mutex_lock(&queue->qmutex)) != 0) {
            FLOG_ERROR("call pthread_mutex_lock fail, "
                       "errno: %d, error info: %s",
                       result, STRERROR(result));
            return NULL;
        }
        pre = NULL;
        cur = queue->head;
        data = NULL;
        while (cur != NULL) {
            if (find_func(cur->data, args)) {
                data = cur->data;

                if (pre == NULL) {
                    queue->head = cur->next;
                    if (queue->head == NULL) {
                        queue->tail = NULL;
                    }
                } else {
                    pre->next = cur->next;
                    if (queue->tail == cur) {
                        queue->tail = pre;
                    }
                }

                queue->count -= 1;

                sf_free_queue_push(cur);
                break;
            }

            pre = cur;
            cur = cur->next;
        }

        if ((result = pthread_mutex_unlock(&queue->qmutex)) != 0) {
            FLOG_ERROR("call pthread_mutex_unlock fail, "
                       "errno: %d, error info: %s",
                       result, STRERROR(result));
        }

        return data;
    } while (false);

    return NULL;
}

static int sf_mem_block_malloc() {
    if (free_queue.mem_block_offset < free_queue.mem_block_count) {
        free_queue.mem_block[free_queue.mem_block_offset] =
            malloc(free_queue.block_size * free_queue.block_count);

        if (free_queue.mem_block[free_queue.mem_block_offset] == NULL) {
            FLOG_ERROR("malloc free_queue %d'th mem block offset "
                       "%d bytes fail, "
                       "errno: %d, error info: %s",
                       free_queue.mem_block_offset,
                       free_queue.block_size * free_queue.block_count, errno,
                       STRERROR(errno));
            return ENOMEM;
        }

    } else {
        FLOG_ERROR("malloc free_queue %d'th mem block offset fail",
                   free_queue.mem_block_offset);

        return ENOMEM;
    }

    char *it = (char *)(free_queue.mem_block[free_queue.mem_block_offset]);

    for (int i = 0; i < free_queue.block_count; i++) {
        sf_queue_entry_t *entry = (void *)it;

        entry->next = NULL;
        if (free_queue.tail == NULL) {
            free_queue.head = entry;
        } else {
            free_queue.tail->next = entry;
        }
        free_queue.tail = entry;

        it += free_queue.block_size;
    }

    free_queue.mem_block_offset += 1;

    return 0;
}

void sf_queue_check_wait(sf_queue_t *queue) {
    struct timespec ts;
    pthread_mutex_lock(&queue->qmutex);
    if (queue->count <= 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5;
        pthread_cond_timedwait(&queue->qcond, &queue->qmutex, &ts);
    }
    pthread_mutex_unlock(&queue->qmutex);
}

void sf_queue_notify(sf_queue_t *queue) { pthread_cond_signal(&queue->qcond); }
