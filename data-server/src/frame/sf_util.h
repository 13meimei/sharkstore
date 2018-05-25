#ifndef __SF_UTIL_H__
#define __SF_UTIL_H__

#include <assert.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

int64_t getticks();
int64_t get_micro_second();
int64_t get_second();

void set_thread_name(pthread_t handle, const char *name);
#ifdef __cplusplus
}
#endif

#endif  //__SF_UTIL_H__
