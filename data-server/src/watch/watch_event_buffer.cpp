//
// Created by zhangyongcheng on 18-8-7.
//

#include "watch_event_buffer.h"



namespace sharkstore {
namespace dataserver {
namespace watch {

void printBufferValue(CEventBufferValue &val) {
    FLOG_DEBUG("key:%s value:%s version:%"
                       PRId64,
               EncodeToHexString(val.key(0)).c_str(), EncodeToHexString(val.value()).c_str(),
               val.version());
}

bool operator < (const struct SGroupKey &l, const struct SGroupKey &r) {
    return l.key_ < r.key_;
}

bool CEventBuffer::thread_flag_=true;
int32_t CEventBuffer::milli_timeout_ = EVENT_BUFFER_TIME_OUT;

CEventBuffer::CEventBuffer() {
    mapGroupBuffer.clear();
    create_thread();
}

CEventBuffer::~CEventBuffer() {
    for(auto it : mapGroupBuffer) {
        delete it.second;
    }
    loop_flag_ = false;
    clear_thread_.join();
}

int32_t CEventBuffer::loadFromBuffer(const std::string &grpKey,  int64_t userVersion,
                                     std::vector<CEventBufferValue> &result) {

    std::lock_guard<std::mutex> lock(buffer_mutex_);

    int32_t resultCnt{0};

    if(isEmpty() || userVersion == 0) {
        return -1;
    }

    auto it = mapGroupBuffer.find(grpKey);
    if(it != mapGroupBuffer.end()) {
        //to do 遍历获取版本范围内变更
        resultCnt = it->second->getData(userVersion, result);
    }

    return resultCnt;
}

bool CEventBuffer::enQueue(const std::string &grpKey, const CEventBufferValue *bufferValue) {

    std::lock_guard<std::mutex> lock(buffer_mutex_);
    bool ret{false};
    int64_t queueLength(0);
    GroupKey key(grpKey);

    auto it = mapGroupBuffer.find(key);
    if(it == mapGroupBuffer.end()) {

        //to do escasp from map
        if(isFull()) {
            GroupKey k(listGroupBuffer.begin()->key_, listGroupBuffer.begin()->create_time_);
            listGroupBuffer.pop_front();

            FLOG_INFO("list pop key:%s", EncodeToHexString(k.key_).c_str());

            auto itMap = mapGroupBuffer.find(k);
            if(itMap != mapGroupBuffer.end()) {
                deQueue(itMap->second);
                mapGroupBuffer.erase(itMap);
                map_size_--;
                FLOG_WARN("map pop success, key:%s  create(ms):%" PRId64 " map-length:%" PRId32, k.key_.c_str(), k.create_time_, map_size_);
            } else {
                FLOG_WARN("map pop error, key:%s  create(ms):%" PRId64 " map-length:%" PRId32, k.key_.c_str(), k.create_time_, map_size_);
            }
        }

        auto grpValue = new GroupValue(MAX_EVENT_QUEUE_SIZE);

        if(grpValue->enQueue(*bufferValue)) {
            listGroupBuffer.push_back(key);
            auto result = mapGroupBuffer.emplace(std::make_pair(key, grpValue));
            if(result.second) {
                ret = true;
            } else {
                FLOG_WARN("mapGroupBuffer emplace error, key:%s", EncodeToHexString(grpKey).c_str());
                ret = false;
            }
            queueLength = grpValue->length();

            if(ret)
                map_size_++;

        } else {
            FLOG_WARN("map[%s]->queue is full.", EncodeToHexString(grpKey).c_str());
            ret = false;
        }
    } else {
        ret = it->second->enQueue(*bufferValue);
        if(!ret) {
            FLOG_WARN("map[%s]->queue is full..", EncodeToHexString(grpKey).c_str());
        }
        queueLength = it->second->length();
    }

    FLOG_DEBUG("emplace to queue,[%d] key:%s value:%s version:%" PRId64 " map-length:%" PRId32 " queue-length:%" PRId64, ret,
               EncodeToHexString(grpKey).c_str(), bufferValue->value().c_str(), bufferValue->version(), map_size_, queueLength);

    return ret;
}

bool CEventBuffer::deQueue(GroupValue   *grpVal) {

    //std::lock_guard<std::mutex> lock(buffer_mutex_);
    if(grpVal != nullptr) {
        grpVal->clearQueue();
        delete grpVal;
    }

    return true;
}


}
}
}