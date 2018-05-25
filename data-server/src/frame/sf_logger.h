#ifndef FBASE_DATA_SERVER_COMMON_LOGGER_H_
#define FBASE_DATA_SERVER_COMMON_LOGGER_H_

#include <fastcommon/logger.h>
#include <stdarg.h>

#define FLOG_DEBUG(fmt, ...) \
    do {\
        if (g_log_context.log_level >= LOG_DEBUG) {\
            logDebug("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__);\
        }\
    } while (false)

#define FLOG_INFO(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_INFO) { \
            logInfo("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__); \
        }\
    } while (false)

#define FLOG_WARN(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_WARNING) { \
            logWarning("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__);\
        }\
    } while (false)

#define FLOG_ERROR(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_ERR) { \
            logError("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__);\
        }\
    } while (false)

#define FLOG_CRIT(fmt, ...) \
    do  {\
        if (g_log_context.log_level >= LOG_CRIT) { \
            logCrit("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__);\
        }\
    } while (false)

#endif /* end of include guard: FBASE_DATA_SERVER_COMMON_LOGGER_H_ */
