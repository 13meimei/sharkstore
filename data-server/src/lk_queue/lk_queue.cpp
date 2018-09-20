#include "lk_queue.h"

#ifndef TBB
#pragma message("Use moodycamel!")

#include "concurrentqueue.h"
struct lock_free_queue_s {
    moodycamel::ConcurrentQueue<void *> lk_queue;
};

bool lk_queue_push(lock_free_queue_t *lk_queue, void *entry) {
    return lk_queue->lk_queue.enqueue(entry);
}

void *lk_queue_pop(lock_free_queue_t *lk_queue) {
    void *item;
    if (lk_queue->lk_queue.try_dequeue(item)) {
        return item;
    }

    return nullptr;
}

#else
#pragma message("Use tbb!")

#include <tbb/concurrent_queue.h>
struct lock_free_queue_s {
    tbb::concurrent_queue<void *> lk_queue;
};

bool lk_queue_push(lock_free_queue_t *lk_queue, void *entry) {
    lk_queue->lk_queue.push(entry);
    return true;
}

void *lk_queue_pop(lock_free_queue_t *lk_queue) {
    void *item;
    if (lk_queue->lk_queue.try_pop(item)) {
        return item;
    }

    return nullptr;
}
#endif

lock_free_queue_t *new_lk_queue() {
    return new lock_free_queue_t;
}

void delete_lk_queue(lock_free_queue_t *lk_queue) {
    delete lk_queue;
}
