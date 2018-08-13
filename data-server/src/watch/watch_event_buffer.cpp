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

//ms
#define EVENT_BUFFER_TIME_OUT 10000
#define MAX_EVENT_QUEUE_SIZE  10000

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

    if(isEmpty()) {
        return resultCnt;
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

    auto it = mapGroupBuffer.find(grpKey);
    if(it == mapGroupBuffer.end()) {
        auto grpValue = new GroupValue(MAX_EVENT_QUEUE_SIZE);

        if(grpValue->enQueue(*bufferValue)) {
            auto result = mapGroupBuffer.emplace(std::make_pair(grpKey, grpValue));
            if(result.second) {
                ret = true;
            } else {
                ret = true;
            }
            queueLength = grpValue->length();
        } else {
            FLOG_WARN("map[%s] is full.", EncodeToHexString(grpKey).c_str());
            ret = false;
        }
    } else {
        ret = it->second->enQueue(*bufferValue);
        queueLength = it->second->length();
    }

    FLOG_DEBUG("emplace to queue,[%d] key:%s value:%s version:%" PRId64 " queue_length:%" PRId64, ret,
               EncodeToHexString(grpKey).c_str(), EncodeToHexString(bufferValue->value()).c_str(), bufferValue->version(), queueLength);

    return ret;
}

bool CEventBuffer::deQueue(const std::string &grpKey, CEventBufferValue *bufferValue) {

    std::lock_guard<std::mutex> lock(buffer_mutex_);

    auto it = mapGroupBuffer.find(grpKey);
    if(it == mapGroupBuffer.end()) {
        return false;
    } else {
        it->second->deQueue(*bufferValue);
    }

    return true;
}


}
}
}