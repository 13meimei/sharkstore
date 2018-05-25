//mc_types.h

#ifndef __DS_TYPES_H__
#define __DS_TYPES_H__

#include "ds_define.h"

typedef struct ds_status_s {
    struct {
        int64_t alloc_count;
        int64_t current_count;
        int64_t max_count;
    } connection;

    struct {
        volatile int64_t total_count;
        volatile int64_t delay_count;
        volatile int64_t no_consumer_count;  //because no consumer
        volatile int64_t discard_count;      //because connection closed
        volatile int64_t waiting_count;
        volatile int64_t max_waiting_count;
        volatile int64_t sent_count;
        volatile int64_t expired_count;
        volatile int64_t consumer_overflows; //consumer queue overflow count
        volatile int64_t user_overflows;     //user queue overflow count
        volatile int64_t repush_count;       //push back again count
    } message;

    struct {
        volatile int64_t server_total_count;
        volatile int64_t server_success_count;
        volatile int64_t client_total_count;
        volatile int64_t client_success_count;
    } ping;

} ds_status_t;

#endif//__DS_TYPES_H__

