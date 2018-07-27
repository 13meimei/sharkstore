_Pragma("once");
#include <string>
#include <proto/gen/watchpb.pb.h>
#include <proto/gen/funcpb.pb.h>
#include <proto/gen/errorpb.pb.h>
#include <common/socket_session.h>
#include <common/ds_encoding.h>
#include <common/ds_config.h>
#include <common/ds_proto.h>

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
    
    static int16_t  NextComparableBytes(const char *key, const int32_t &len, std::string &result);
};

enum WATCH_CODE {
    WATCH_OK = 0,
    WATCH_KEY_NOT_EXIST = -1,
    WATCH_WATCHER_NOT_EXIST = -2
};

class WatchProtoMsg: public common::ProtoMessage {
public:
    /*WatchProtoMsg(){};
    ~WatchProtoMsg(){};

    WatchProtoMsg(const WatchProtoMsg &) = delete;
    WatchProtoMsg &operator=(const WatchProtoMsg &) = delete;
    WatchProtoMsg &operator=(const WatchProtoMsg &) volatile = delete;
    */
public:
    bool getFlag() {
        return send_flag_;
    }

    void setFlag() {
        send_flag_ = true;
    }

private:
    bool send_flag_ = false;

};

typedef std::unordered_map<int64_t, WatchProtoMsg *> WatcherSet_;
typedef std::unordered_map<std::string, int16_t> KeySet_;

typedef std::unordered_map<std::string, WatcherSet_*> Key2Watchers_;
typedef std::unordered_map<int64_t, KeySet_*> Watcher2Keys_;

//timer queue class
class Watcher{
public:
    Watcher() {};
    Watcher(WatchProtoMsg* msg, const std::string &key) {
        this->msg_ = std::make_shared<WatchProtoMsg>(*msg);
        this->key_ = key;
    }
    ~Watcher() {
        if(msg_->getFlag()) {
            ;
        }
    }
    bool operator<(const Watcher& other) const;

    bool Send(std::shared_ptr<WatchProtoMsg> msg, google::protobuf::Message* resp) {
        // // 分配回应内存
        size_t body_len = resp == nullptr ? 0 : resp->ByteSizeLong();
        size_t data_len = header_size + body_len;

        response_buff_t *response = new_response_buff(data_len);

        // 填充应答头部
        ds_header_t header;
        header.magic_number = DS_PROTO_MAGIC_NUMBER;
        header.body_len = body_len;
        header.msg_id = msg->header.msg_id;
        header.version = DS_PROTO_VERSION_CURRENT;
        header.msg_type = DS_PROTO_FID_RPC_RESP;
        header.func_id = msg->header.func_id;
        header.proto_type = msg->header.proto_type;

        ds_serialize_header(&header, (ds_proto_header_t *)(response->buff));

        response->session_id  = msg->session_id;
        response->msg_id      = header.msg_id;
        response->begin_time  = msg->begin_time;
        response->expire_time = msg->expire_time;
        response->buff_len    = data_len;

        std::lock_guard<std::mutex> lock(mutex_);
        if(msg->getFlag()) {
            FLOG_DEBUG("Already sended, session_id[%" PRIu64 "]", msg->session_id);
            return false;
        }

        do {
            if (resp != nullptr) {
                char *data = response->buff + header_size;
                if (!resp->SerializeToArray(data, body_len)) {
                    FLOG_ERROR("serialize response failed, func_id: %d", header.func_id);
                    delete_response_buff(response);
                    break;
                }
            }

            //处理完成，socket send
            msg->socket->Send(response);

        } while (false);

        msg->setFlag();
        return true;
        //delete msg;
        //delete resp;
    }

public:
    std::shared_ptr<WatchProtoMsg> msg_;
    std::string key_;

    std::mutex mutex_;
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

//base class
class WatcherSet {
public:
    WatcherSet();
    ~WatcherSet();
    int32_t AddWatcher(std::string &, WatchProtoMsg*);
    WATCH_CODE DelWatcher(int64_t, const std::string &);
    uint32_t GetWatchers(std::vector<WatchProtoMsg*>& , std::string &);
    
    uint64_t GetWatcherSize() {
        return static_cast<uint32_t>(key_index_.size());
    }

private:
    Key2Watchers_ key_index_;
    Watcher2Keys_ watcher_index_;
    std::mutex mutex_;

    TimerQueue<std::shared_ptr<Watcher>> timer_;
    std::condition_variable timer_cond_;
    std::thread watchers_expire_thread_;
    volatile bool watchers_expire_thread_continue_flag = true;

};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
