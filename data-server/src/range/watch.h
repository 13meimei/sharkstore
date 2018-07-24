_Pragma("once");
#include <string>
#include <proto/gen/watchpb.pb.h>
#include <proto/gen/funcpb.pb.h>
#include <proto/gen/errorpb.pb.h>
#include <common/socket_session.h>
#include <common/ds_encoding.h>
#include <common/ds_config.h>

#include <frame/sf_logger.h>
#include <mutex>
#include <memory>
#include <queue>
#include <vector>
#include <functional>
#include <condition_variable>
#include <thread>
#include <unordered_map>

#define MAX_WATCHER_SIZE 100000

namespace sharkstore {
namespace dataserver {
namespace range {

bool DecodeWatchKey(std::vector<std::string*>& keys, std::string* buf);
bool DecodeWatchValue(int64_t *version, std::string *value, std::string *extend,
                      std::string &buf);
void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);
void EncodeWatchValue(std::string *buf,
                      int64_t &version,
                      const std::string *value,
                      const std::string *extend);

class WatchCode {
public:
    WatchCode() {};

    ~WatchCode() {};

public:
    static int16_t EncodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err);

    static int16_t DecodeKv(funcpb::FunctionID funcId, const metapb::Range &meta_, watchpb::WatchKeyValue *kv,
                            std::string &db_key, std::string &db_value,
                            errorpb::Error *err);
    
    static int16_t  NextComparableBytes(const char *key, const int16_t len, std::string &result);
};

enum WATCH_CODE {
    WATCH_OK = 0,
    WATCH_KEY_NOT_EXIST = -1,
    WATCH_WATCHER_NOT_EXIST = -2
};

typedef std::unordered_map<int64_t, common::ProtoMessage*> WatcherSet_;
typedef std::unordered_map<std::string, int16_t> KeySet_;

typedef std::unordered_map<std::string, WatcherSet_*> Key2Watchers_;
typedef std::unordered_map<int64_t, KeySet_*> Watcher2Keys_;

struct Watcher{
    Watcher() {};
    Watcher(common::ProtoMessage* msg, std::string key) {
        this->msg_ = msg;
        this->key_ = key;
    }
    ~Watcher() = default;
    bool operator<(const Watcher& other) const;

    common::ProtoMessage* msg_;
    std::string key_;
};

template <class T>
struct Greater {
    bool operator()(const T& a, const T& b) {
        return a < b;
    }
};

template <class T>
struct TimerQueue: public std::priority_queue<T, std::vector<T>, Greater<T>> {
public:
    std::vector<T>& GetQueue() { return this->c; }
};

class WatcherSet {
public:
    WatcherSet();
    ~WatcherSet();
    int32_t AddWatcher(std::string &, common::ProtoMessage*);
    WATCH_CODE DelWatcher(int64_t, const std::string &);
    uint32_t GetWatchers(std::vector<common::ProtoMessage*>& , std::string &);

private:
    Key2Watchers_ key_index_;
    Watcher2Keys_ watcher_index_;
    std::mutex mutex_;

    TimerQueue<Watcher> timer_;
    std::condition_variable timer_cond_;
    std::thread watchers_expire_thread_;
    volatile bool watchers_expire_thread_continue_flag = true;

};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
