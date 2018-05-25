#include "lk_queue.h"

#include "concurrentqueue.h"

extern "C" {

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

lock_free_queue_t *new_lk_queue() {
    return new lock_free_queue_t;
}

void delete_lk_queue(lock_free_queue_t *lk_queue) {
    delete lk_queue;
}

} // end extern "C"
