//
// Created by zhangyongcheng on 18-8-7.
//
_Pragma("once");

#include "circular_queue.h"
#include "proto/gen/watchpb.pb.h"
#include "frame/sf_logger.h"
#include "common/ds_encoding.h"
#include "frame/sf_util.h"

#include <mutex>
#include <thread>
#include <condition_variable>

namespace sharkstore {
    namespace dataserver {
        namespace watch {

            class CEventBufferValue;
            void printBufferValue(CEventBufferValue &val);

            class CEventBufferValue {
            public:
                CEventBufferValue(){};
                CEventBufferValue &operator=(const CEventBufferValue &oth) {
                    this->key_.assign(oth.key_.begin(), oth.key_.end());
                    this->value_ = oth.value_;
                    this->version_ = oth.version_;
                    this->evtType_ = oth.evtType_;
                    this->create_time_ = getticks();
                    return *this;
                }

                CEventBufferValue(const watchpb::WatchKeyValue &val, const watchpb::EventType &evtType) {
                    CopyFrom(val);
                    this->evtType_ = evtType;
                    this->create_time_ = getticks();
                }

                const int64_t &version() const {
                    return version_;
                }
                int32_t key_size() const {
                    return key_.size();
                }
                const std::vector<std::string> &key() const {
                    return key_;
                }
                const std::string &key(uint32_t idx) const {
                    assert(key_.size() > idx);
                    return key_[idx];
                }
                const std::string &value() const {
                    return value_;
                }
                const watchpb::EventType &type() const {
                    return evtType_;
                }
                int64_t createTime() const {
                    return create_time_;
                }

                private:
                void CopyFrom(const watchpb::WatchKeyValue &val) {
                    for(auto it : val.key()) {
                        key_.emplace_back(it);
                    }
                    value_ = val.value();
                    version_ = val.version();
                }

                watchpb::EventType evtType_{watchpb::PUT};
                std::vector<std::string> key_;
                std::string value_;
                int64_t version_;
                int64_t create_time_;

            };

            using GroupValue = CircularQueue<CEventBufferValue>;



            class CEventBuffer {
            public:
                CEventBuffer();
                ~CEventBuffer();

                int32_t loadFromBuffer(const std::string &grpKey, int64_t uerVersion, std::vector<CEventBufferValue> &result);

                bool enQueue(const std::string &grpKey, const CEventBufferValue *bufferValue);

                bool deQueue(const std::string &grpKey, CEventBufferValue *bufferValue);

                void clear(const std::string &grpKey) {
                    auto it = mapGroupBuffer.find(grpKey);
                    if (it != mapGroupBuffer.end()) {
                        it->second->clearQueue();
                        mapGroupBuffer.erase(it);
                    }
                }

                bool isEmpty() {
                    return (mapGroupBuffer.size() == 0);
                }

                void create_thread() {
                    if(thread_flag_) {
                        thread_flag_ = false;
                        clear_thread_ = std::thread([this]() {
                            while(loop_flag_) {
                                std::unique_lock<std::mutex> lock(buffer_mutex_);
                                //to do pop timeout data from queue
                                for(auto itMap:mapGroupBuffer) {
                                    if(itMap.second->isEmpty())
                                        continue;

                                    int32_t popCnt{0};
                                    for(auto i = 0; i < itMap.second->length(); i++) {
                                        if (getticks() - itMap.second->preDequeue() > milli_timeout_) {
                                            CEventBufferValue elem;
                                            itMap.second->deQueue(elem);
                                            popCnt++;
                                        } else {
                                            break;
                                        }
                                    }
                                    FLOG_INFO("key:%s pop number:%" PRId32 , EncodeToHexString(itMap.first).c_str(), popCnt);

                                    usleep(50000);
                                }

                                usleep(1000000);
                            }
                        });
                    }
                }


            private:
                std::map<std::string, GroupValue *> mapGroupBuffer;

                std::mutex buffer_mutex_;
                std::condition_variable cond_;

                static int32_t milli_timeout_;
                static bool thread_flag_;
                std::thread clear_thread_;
                volatile bool loop_flag_{true};

            };


        }
    }
}

