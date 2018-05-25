#ifndef __SF_QUEUE_H__
#define __SF_QUEUE_H__

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct sf_queue_entry_s sf_queue_entry_t;

typedef bool (*sf_queue_find_t) (void *it, void *args);

struct sf_queue_entry_s {
    void  *data;

    sf_queue_entry_t  *next;
};

typedef struct sf_queue_s {
    sf_queue_entry_t *head;
    sf_queue_entry_t *tail;

    int count;

    pthread_mutex_t qmutex;
    pthread_cond_t  qcond;
} sf_queue_t;

typedef struct sf_free_queue_s{
    int     block_size;
    int     block_count;

    int     mem_block_offset;
    int     mem_block_count;

    sf_queue_entry_t **mem_block;

    pthread_mutex_t     lock;
    sf_queue_entry_t    *head;
    sf_queue_entry_t    *tail;
} sf_free_queue_t;


#ifdef __cplusplus
extern "C" {
#endif

int sf_free_queue_init(int init_count, int max_count);
void sf_free_queue_destroy();

int sf_free_queue_push();
sf_queue_entry_t *sf_free_queue_pop();

int sf_queue_init(sf_queue_t *queue);
int sf_queue_push(sf_queue_t *queue, void *entry);

void *sf_queue_pop(sf_queue_t *queue);
void *sf_queue_find_if(sf_queue_t *queue, sf_queue_find_t find_func, void *args);


void sf_queue_check_wait(sf_queue_t *queue);
void sf_queue_notify(sf_queue_t *queue);

/*
static bool sf_queue_empty(sf_queue_t *queue)
{
    return queue->count == 0;
}
*/
#ifdef __cplusplus
}
#endif

#endif//__SF_QUEUE_H__
