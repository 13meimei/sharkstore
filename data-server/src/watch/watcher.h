#ifndef _WATCHER_H_
#define _WATCHER_H_

#include <unordered_map>

#include "common/socket_session.h"

namespace sharkstore {
namespace dataserver {
namespace watch {

class Watcher {
public:
    Watcher() = delete;
    Watcher(common::ProtoMessage* msg);
    virtual ~Watcher() = default;
    bool operator>(const Watcher* other) const;

private:
    std::string name;
    common::ProtoMessage* message_ = nullptr;

    std::mutex          send_lock_;
    volatile bool       sent_response_flag = false;

public:
    common::ProtoMessage* GetMessage() { return message_; }
    bool IsSentResponse() { return sent_response_flag; }

public:
    virtual void Send(google::protobuf::Message* resp);

    virtual bool DecodeKey(std::vector<std::string*>& keys,
                           const std::string& buf);
    virtual bool DecodeValue(int64_t* version, std::string* value, std::string* extend,
                             std::string& buf);
    virtual void EncodeKey(std::string* buf,
                                uint64_t tableId, const std::vector<std::string*>& keys);
    virtual void EncodeValue(std::string* buf,
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

// todo
class KeyWatcher: public Watcher {

};

class PrefixWatcher: public Watcher {

};



} // namespace watch
}
}

#endif
