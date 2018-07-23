_Pragma("once");

#include <atomic>
#include "rocksdb/db.h"
#include "base/status.h"
#include "base/util.h"
#include "storage/meta_store.h"
#include "storage/store.h"

#define DEFAULT_CACHE_SIZE 10000
static const std::string kRangeVersionKey = "\x05VerID";

namespace sharkstore {

class IdGenerater {
public:
    IdGenerater() {
        ;
    }
    IdGenerater(const uint64_t &range_id, const uint32_t &cache, sharkstore::dataserver::storage::Store *store):
                range_id_(range_id),cache_(cache),store_(store),saveCnt(0) {
        init();
    }

    ~IdGenerater(){
        //to do save number to rocksdb
        store_ = nullptr;
    }

    void init() {
        //to do get from rocksdb and save newId to db
        initFlag = false;
        range_key_ = kRangeVersionKey+std::to_string(range_id_);

        Status ret;
        std::string value("");
        ret = store_->Get(range_key_, &value);
        if(ret.ok()) {
            seq_ = atoi(value.c_str());

            if(cache_ <= 0 ) setCache();
            seq_end_ = seq_ + cache_;

            if( 0 == saveToDb(seq_end_) ) initFlag = true;
        } else if(ret.code() == Status::kNotFound) {
            //to do put new value into db
            seq_end_ = cache_;
            seq_.fetch_add(1);
            if( 0 == saveToDb(seq_end_) ) initFlag = true;
        } else {
            initFlag = false;
            setErrMsg(ret.ToString());
        }

        return;
    }

    void setRangeId(const uint64_t &rangeId) {
        range_id_ = rangeId;
    }
    uint64_t &getRangeId() {
        return range_id_;
    }
    void setCache() {
        setCache(DEFAULT_CACHE_SIZE);
    }
    void setCache(const uint32_t &cacheNum) {
        cache_ = cacheNum;
    }

    int16_t nextId(int64_t *id ) {
        int16_t ret(0);
        
        if (initFlag) {
            if (id == nullptr) {
                id = new int64_t(0L);
            }

            *id = seq_.fetch_add(1);

            if (*id >= seq_end_) {
                seq_end_ = *id + cache_;
                ret = saveToDb(seq_end_);

                if (0 == ret) {
                    saveCnt++;
                }
            }

        } else {
            setErrMsg( "Generator is not init yet or encournter init failure." );
            ret = -1;
        }

        return ret;
    }

    std::string &getErrMsg() {
        return errMsg;
    }
    void clearErrMsg() noexcept {
        errMsg.assign("");
    }
    
    bool isInit() {
        return initFlag;
    }
private:
    int16_t saveToDb(const int64_t &futureId) {
        std::unique_lock<std::mutex> lock(mtx_);
        int16_t retCode(0);

        if (initFlag) {
            //to do save to rocksdb
            Status ret = store_->Put(range_key_, std::to_string(futureId));
            if (!ret.ok()) {
                setErrMsg( ret.ToString() );
            }
            if (ret.ok()) {
                retCode = 0;
            } else {
                retCode = -1;
            }
        } else {
            setErrMsg("Generator is not init yet.");
            retCode = -1;
        }

        return retCode;
    }

    void setErrMsg(const std::string &msg) {
        errMsg.assign(msg);
    }

private:
    std::atomic<int64_t> seq_;
    int64_t seq_end_ = 0;
    uint32_t cache_ = 0;

    uint64_t range_id_ = 0;
    std::string range_key_;

    sharkstore::dataserver::storage::Store *store_ = nullptr;
    
    bool initFlag = false;
    std::string errMsg;
    int32_t saveCnt = 0;

    std::mutex mtx_;
};

} //end namespace
