#ifndef __LOCK_FREE_QUEUE_H__
#define __LOCK_FREE_QUEUE_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lock_free_queue_s lock_free_queue_t;

bool lk_queue_push(lock_free_queue_t *lk_queue, void *entry);
void *lk_queue_pop(lock_free_queue_t *lk_queue);

lock_free_queue_t *new_lk_queue();
void delete_lk_queue(lock_free_queue_t *lk_queue);

#ifdef __cplusplus
}
#endif


#endif//__LOCK_FREE_QUEUE_H__
