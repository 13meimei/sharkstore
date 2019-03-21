#include <gtest/gtest.h>
#include "helper/cpp_permission.h"

#include <fastcommon/shared_func.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "helper/mock/raft_server_mock.h"
#include "helper/mock/rpc_request_mock.h"

#include "range/range.h" 
#include "watch/watcher.h"
#include "test_public_funcs.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);

char level[8] = "debug"; 

int main(int argc, char *argv[]) { 

    if(argc > 1)
        strcpy(level, argv[1]);

    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}


#define VEC_SIZE 100

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;
using namespace sharkstore::test::mock;

std::string  DecodeSingleKey(const int16_t grpFlag, const std::string &encodeBuf) {
    std::vector<std::string *> vec;
    std::string key("");
    auto buf = new std::string(encodeBuf);

    watch::Watcher watcher(1, vec);
    watcher.DecodeKey(vec, encodeBuf);

    if(grpFlag) {
        for(auto it:vec) {
            key.append(*it);
        }
    } else {
        key.assign(*vec[0]);
    }
    
   //     FLOG_DEBUG("DecodeWatchKey exception(%d), %s", int(vec.size()), EncodeToHexString(*buf).c_str());

    if(vec.size() > 0 && key.empty())
        key.assign(*vec[0]);

    FLOG_DEBUG("DecodeKey: %s", key.c_str());
    return key;
}

class WatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.engine_config.name, "rocksdb");
        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(NowMilliSeconds()).c_str());
        ds_config.range_config.recover_concurrency = 10; 
        ds_config.watch_config.watcher_thread_priority = 23;

        range_server_ = new server::RangeServer; 
        context_ = new server::ContextServer; 
        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);

        for(auto i = 0; i++ < VEC_SIZE;) {
            vec_.push_back(i);
        } 

        {
            // begin test create range
            schpb::CreateRangeRequest req;
            req.set_allocated_range(genRange1());
            auto rpc = NewMockRPCRequest(req);

            range_server_->CreateRange(*rpc.first);
            ASSERT_FALSE(range_server_->ranges_.empty());

            ASSERT_TRUE(range_server_->Find(1) != nullptr);

            std::vector<metapb::Range> metas;
            auto ret = range_server_->meta_store_->GetAllRange(&metas);

            ASSERT_TRUE(metas.size() == 1) << metas.size();
            // end test create range
        }

        {
            // begin test create range
            schpb::CreateRangeRequest req;
            req.set_allocated_range(genRange2());
            auto rpc = NewMockRPCRequest(req);

            range_server_->CreateRange(*rpc.first);
            ASSERT_FALSE(range_server_->ranges_.empty());

            ASSERT_TRUE(range_server_->Find(2) != nullptr);

            std::vector<metapb::Range> metas;
            auto ret = range_server_->meta_store_->GetAllRange(&metas);

            ASSERT_TRUE(metas.size() == 2) << metas.size();
            // end test create range
        } 
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

    void justPut(const int16_t &rangeId, const std::string &key1, const std::string &key2,const std::string &value)
    {
        FLOG_DEBUG("justPut...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , value.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        watchpb::DsKvWatchPutRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2);
        req.mutable_req()->mutable_kv()->set_value(value);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchPut);
        rpc.first->expire_time = NowMicros() + 1000000;
        rpc.first->msg->head.msg_id = 20180813;
        rpc.first->begin_time = NowMicros();

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchPutResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        FLOG_DEBUG("watch_put first response: %s", resp.DebugString().c_str());
        ASSERT_TRUE(resp.resp().code() == 0);

        return;
    }

    void justDel(const int16_t &rangeId, const std::string &key1, const std::string &key2, const std::string &value, const int16_t& existFlag = 0, bool prefix = false)
    {
        FLOG_DEBUG("justDel...range:%d key1:%s  key2:%s", rangeId, key1.c_str(), key2.c_str() );

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[1]->setLeaderFlag(true);

        watchpb::DsKvWatchDeleteRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_req()->mutable_kv()->add_key(key2); 
        req.mutable_req()->mutable_kv()->set_version(1); 
        req.mutable_req()->set_prefix(prefix);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchDel);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813;

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchDeleteResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        FLOG_WARN("DEL resp.code:%d", resp.resp().code());
        FLOG_DEBUG("watch_del response: %s", resp.DebugString().c_str()); 
        ASSERT_TRUE(resp.resp().code() == existFlag); 
    }

    void justGet(const int16_t &rangeId, const std::string key1, const std::string &key2, const std::string& val, const int32_t& cnt, bool prefix = false)
    {
        FLOG_DEBUG("justGet...range:%d key1:%s  key2:%s  value:%s", rangeId, key1.c_str(), key2.c_str() , val.c_str());

        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->setLeaderFlag(true);

        watchpb::DsKvWatchGetMultiRequest req; 
        req.set_prefix(prefix);
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1); 
        req.mutable_kv()->add_key(key1);
        if(!key2.empty())
            req.mutable_kv()->add_key(key2);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncPureGet);
        rpc.first->expire_time = NowMilliSeconds() + 1000;
        rpc.first->begin_time = NowMicros();

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsKvWatchGetMultiResponse resp;
        auto s = rpc.second->Get(resp);
        
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()); 
        FLOG_DEBUG(">>>PureGet RESP:%s", resp.DebugString().c_str()); 
        EXPECT_TRUE(resp.kvs_size() == cnt);

        if (cnt && resp.kvs_size()) {
            for(auto i = 0; i<cnt; i++)
                EXPECT_TRUE(resp.kvs(i).value() == val);
        }
    }

    void justWatch(const int16_t &rangeId, const std::string key1, const std::string key2, const int64_t &timeout, const int64_t &version = 0, bool prefix = false)
    {
        FLOG_DEBUG("justWatch...range:%d key1:%s  key2:%s  prefix:%d", rangeId, key1.c_str(), key2.c_str(), prefix );
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[rangeId]->raft_.get());
        raft->ops_.leader = 1;
        raft->SetLeaderTerm(1, 1);
        range_server_->ranges_[rangeId]->is_leader_ = true;

        watchpb::DsWatchRequest req; 
        req.mutable_header()->set_range_id(rangeId);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1); 
        req.mutable_req()->mutable_kv()->add_key(key1);
        if(!key2.empty()) {
            req.mutable_req()->mutable_kv()->add_key(key2);
        }
        //req.mutable_req()->mutable_kv()->set_version(version);
        req.mutable_req()->set_longpull(timeout);
        ///////////////////////////////////////////////
        req.mutable_req()->set_startversion(version);
        req.mutable_req()->set_prefix(prefix);

        auto rpc = NewMockRPCRequest(req, funcpb::kFuncWatchGet);
        rpc.first->expire_time = NowMilliSeconds() + 3000;
        rpc.first->begin_time = NowMicros();
        rpc.first->msg->head.msg_id = 20180813;

        range_server_->DealTask(std::move(rpc.first));

        watchpb::DsWatchResponse resp;
        auto s = rpc.second->Get(resp);
        
//        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error());
        //ASSERT_TRUE(resp.header().error().has_key_not_in_range()); 
    } 

    std::thread trd1;
    std::thread trd2;
    std::condition_variable cond_;
    std::mutex mutex_;

    std::vector<int64_t> vec_;
    std::vector<std::string> str_vec_;
    std::atomic<int32_t> cnt_; 
protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
}; 

TEST_F(WatchTest, watch_exist_singlekey_test) {
    justPut(1, "01003001", "", "01003001:value");
    justWatch(1, "01003001", "", 5000, 0, false);
    //wait timeout
    sleep(6);

    justWatch(1, "01003001", "", 5000, 0, false);
    justDel(1, "01003001", "", "");

}

TEST_F(WatchTest, watch_notexist_singlekey_test) {
    //justPut(1, "01003001", "", "01003001:value");
    //del exists key
    justDel(1, "01003001", "", "", 0, true);
    //watch not exist key ,expect fail due to version is not 0
    justWatch(1, "01003001", "", 5000, 1, false);

    //watch success
    justWatch(1, "01003001", "", 5000, 0, false);
    //wait timeout response
    sleep(6);

    //watch again
    justWatch(1, "01003001", "", 5000, 1, false);
    justPut(1, "01003001", "", "01003001:value");

}

TEST_F(WatchTest, watch_exist_groupkey_test) {

    //justPut(1, "01003001", "0100300102", "01003001:value");
    justDel(1, "01003001", "", "", 0, true);
    //add prefix watch
    justWatch(1, "01003001", "", 5000, 0, true);
    //delete trigger notify
    justDel(1, "01003001", "0100300102", "", 0);
    //nothing
    justDel(1, "01003001", "0100300102", "", 0);

    //watch not exists key
    justWatch(1, "0100300101", "", 5000, 0, true);
    //put trigger watch
    justPut(1, "0100300101", "0100300102", "01003001:value");
    //del trigger nothing
    justDel(1, "0100300101", "0100300102", "", 0);

}

TEST_F(WatchTest, watch_notexist_groupkey_test) {

    justPut(1, "01003001", "0100300102", "01003001:value");
    //add prefix watch
    justWatch(1, "01003001", "", 5000, 0, true);
    //delete trigger notify
    justDel(1, "01003001", "0100300102", "");
    //nothing
    justDel(1, "01003001", "0100300102", "", 0);

    //watch not exists key
    justWatch(1, "0100300101", "", 5000, 0, true);
    //put trigger watch
    justPut(1, "0100300101", "0100300102", "01003001:value");
    //del trigger nothing
    justDel(1, "0100300101", "0100300102", "");

}

#define watch_put_del_watch_group
#ifdef watch_put_del_watch_group
TEST_F(WatchTest, watch_put_del_watch_group) {

    trd1 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock( mutex_ );

            int32_t element(0);
            if (!vec_.empty()) {
                element = vec_.back();

                FLOG_DEBUG("thread1>>>%" PRId32, element);

                if (element % 2 == 0) {
                    vec_.pop_back();
                    justPut(1, "01003001", "01003001-aaa", "03003001:value");

                    auto version = range_server_->Find(1)->apply_index_;

                    justWatch(1, "01003001", "", 5000, version, true);
                    sleep(6);
                    cnt_.fetch_add(1);
                    //cond_.wait(lock);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }

            } else {
                brkFlag = true;
            }

//            if(cnt_>=100) {
//                FLOG_DEBUG("TRD1 OVER 100");
//                brkFlag = true;
//            }
        }while(!brkFlag);

    });

    trd2 = std::thread([this]() {
        static bool brkFlag(false);
        do {
            std::unique_lock<std::mutex> lock(mutex_);

            int32_t element(0);
            if (!vec_.empty()) {
                element = vec_.back();

                FLOG_DEBUG("thread2>>>%" PRId32, element);

                if (element % 2 != 0) {
                    vec_.pop_back();
                    //justGet(1, "01003001", "", "03003001:value", 1);
                    justDel(1, "01003001", "01003001-aaa", "", 0, true);

                    cnt_.fetch_add(1);
                    cond_.notify_one();
                } else {
                    //cond_.wait_for(lock, std::chrono::milliseconds(1000));
                    cond_.wait(lock);
                }
            } else {
                brkFlag = true;
            }

//            if(cnt_>=100) {
//                FLOG_DEBUG("TRD2 OVER 100");
//                brkFlag = true;
//            }
        }while(!brkFlag);

    });

    trd1.join();
    trd2.join();

    int64_t cnt = cnt_;

    FLOG_DEBUG("cnt:%" PRId64, cnt); 

}
#endif

