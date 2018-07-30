#ifndef _WATCHER_H_
#define _WATCHER_H_

#include <mutex>
#include <unordered_map>

#include "watch.h"
#include "common/socket_session.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

class Watcher {
public:
    Watcher() = delete;
    Watcher(uint64_t, const std::vector<Key*>&, common::ProtoMessage*);
    Watcher(WatchType, uint64_t, const std::vector<Key*>&, common::ProtoMessage*);
    virtual ~Watcher();
    bool operator>(const Watcher* other) const;

private:
    uint64_t                    table_id_;
    std::vector<std::string*>   keys_;
    common::ProtoMessage*       message_ = nullptr;
    WatchType                   type_ = WATCH_KEY;

    std::mutex          send_lock_;
    volatile bool       sent_response_flag = false;

public:
    uint64_t GetTableId() { return table_id_; }
    const std::vector<std::string*>& GetKeys() { return keys_; }
    common::ProtoMessage* GetMessage() { return message_; }
    int GetType() { return type_; }
    bool IsSentResponse() {
        std::lock_guard<std::mutex> lock(send_lock_);
        return sent_response_flag;
    }

public:
    virtual void Send(google::protobuf::Message* resp);

    bool DecodeKey(std::vector<std::string*>& keys,
                   const std::string& buf);
    bool DecodeValue(int64_t* version, std::string* value, std::string* extend,
                     std::string& buf);
    void EncodeKey(std::string* buf,
                   uint64_t tableId, const std::vector<std::string*>& keys);
    void EncodeValue(std::string* buf,
                     int64_t version,
                     const std::string* value,
                     const std::string* extend);

};

template <class T>
struct Greater {
    bool operator()(const T& a, const T& b) {
        return a > b;
    }
};

/*
class KeyWatcher: public Watcher {
public:
    KeyWatcher() = delete;
    KeyWatcher(const Key&, common::ProtoMessage*);
    ~KeyWatcher() = default;

private:
    Key key_;

public:
    const Key& GetKey();
};

class PrefixWatcher: public Watcher {
public:
    PrefixWatcher() = delete;
    PrefixWatcher(const std::vector<Prefix>&, common::ProtoMessage*);
    ~PrefixWatcher() = default;

private:
    Prefix prefix_;

public:
    const Prefix& GetPrefix();
};
*/

typedef std::shared_ptr<Watcher>        WatcherPtr;
//typedef std::shared_ptr<KeyWatcher>     KeyWatcherPtr;
//typedef std::shared_ptr<PrefixWatcher>  PrefixWatcherPtr;


} // namespace watch
}
}

#endif
