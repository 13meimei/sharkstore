#include <gtest/gtest.h>
#include "cpp_permission.h"

#include <fastcommon/shared_func.h>
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "frame/sf_util.h"
#include "proto/gen/watchpb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "range/range.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "storage/store.h"

#include "raft_server_mock.h"
#include "socket_session_mock.h"
#include "range/range.h"

//extern void EncodeWatchKey(std::string *buf, const uint64_t &tableId, const std::vector<std::string *> &keys);

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

char level[8] = "debug";

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::range;
using namespace sharkstore::dataserver::storage;

class WatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id = 1;
        context_->range_server = range_server_;
        context_->socket_session = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);
        now = getticks();
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

protected:
    server::ContextServer *context_;
    server::RangeServer *range_server_;
    int64_t now;
};

metapb::Range *genRange1() {
    auto meta = new metapb::Range;
    
    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01003"), k2("01004");

    keys.push_back(&k1);
    EncodeWatchKey(&keyStart, 1, keys);
    keys.clear();
    keys.push_back(&k2);
    EncodeWatchKey(&keyEnd, 1, keys);

    meta->set_id(1);
    //meta->set_start_key("01003");
    //meta->set_end_key("01004");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    peer = meta->add_peers();
    peer->set_id(2);
    peer->set_node_id(2);

    return meta;
}

metapb::Range *genRange2() {
    auto meta = new metapb::Range;

    std::vector<std::string*> keys;
    keys.clear();
    std::string keyStart("");
    std::string keyEnd("");
    std::string k1("01004"), k2("01005");

    keys.push_back(&k1);
    EncodeWatchKey(&keyStart, 1, keys);
    keys.clear();
    keys.push_back(&k2);

    EncodeWatchKey(&keyEnd, 1, keys);
    meta->set_id(2);
    //meta->set_start_key("01004");
    //meta->set_end_key("01005");
    meta->set_start_key(keyStart);
    meta->set_end_key(keyEnd);

    meta->mutable_range_epoch()->set_conf_ver(1);
    meta->mutable_range_epoch()->set_version(1);

    meta->set_table_id(1);

    auto peer = meta->add_peers();
    peer->set_id(1);
    peer->set_node_id(1);

    return meta;
}

TEST_F(WatchTest, watch) {
    {
        // begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange1());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(1) != nullptr);

        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test create range
    }

    {
        // begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        req.set_allocated_range(genRange2());

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->find(2) != nullptr);

        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 2) << metas.size();
        // end test create range
    }

    {
        // begin test watch_put (no leader)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        range_server_->ranges_[1]->setLeaderFlag(true);
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        //ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        //ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test watch_put
    }

    {
        // begin test watch_put (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->setLeaderFlag(false);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test watch_put
    }

    {
        // begin test watch_put (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        //ASSERT_TRUE(resp.header().error().key_not_in_range().start_key() == "01003");
        EXPECT_TRUE(resp.header().error().key_not_in_range().start_key() == "01003");
        //ASSERT_TRUE(resp.header().error().key_not_in_range().end_key() == "01004");
        EXPECT_TRUE(resp.header().error().key_not_in_range().end_key() == "01004");

        // end test watch_put
    }

    {
        // begin test watch_put (stale epoch)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        //ASSERT_TRUE(resp.header().has_error());
        EXPECT_TRUE(resp.header().has_error());
        //ASSERT_TRUE(resp.header().error().has_stale_epoch());
        EXPECT_TRUE(resp.header().error().has_stale_epoch());

        // end test watch_put
    }

    {
        // begin test watch_put group (key ok)
        FLOG_DEBUG("watch_put group mode.");

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_put
    }

    {
        // begin test watch_put (ok)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_value("01003001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_put
    }

    {
        // begin test watch_put (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->setLeaderFlag(true);
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->setLeaderFlag(true);
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_value("01004001:value");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchPut(msg);

        watchpb::DsKvWatchPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        //ASSERT_FALSE(resp.header().has_error());
        EXPECT_FALSE(resp.header().has_error());

        // end test watch_put
    }
    
    
    std::cout << "pure_get ...ok" << '\n';
    {
        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.kvs_size()) {
            ;
            ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");
        } else {
            std::cout << "no kvs_size" << std::endl;
        }

    }

    FLOG_DEBUG("pure_get ...group ok");
    {
        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->add_key("01003002");
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.kvs_size()) {
            ;
            ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");
        } else {
            std::cout << "no kvs_size" << std::endl;
        }

    }

    FLOG_DEBUG("pure_get ...prefix ok");
    {
        // begin test pure_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchGetMultiRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_kv()->add_key("01003001");
        req.mutable_kv()->set_version(0);
        req.mutable_kv()->set_tableid(1);
        req.set_prefix(true);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->PureGet(msg);

        watchpb::DsKvWatchGetMultiResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.kvs_size()) {
            ;
            ASSERT_TRUE(resp.kvs(0).value() == "01003001:value");
        } else {
            FLOG_DEBUG("no kvs_size");
        }
    }

    
        // end test watch_get
    FLOG_DEBUG("watch_get ... ok");
    {
        // begin test watch_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->set_version(0);
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.resp().events_size()) {
            ;
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");
        }

        // end test watch_get
    }


    FLOG_DEBUG("watch_get group...ok");
    {
        // begin test watch_get(ok)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        msg->session_id = 1;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->mutable_kv()->add_key("01003002");
        req.mutable_req()->mutable_kv()->set_version(0);
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.resp().events_size()) {
            ;
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");
        }

        // end test watch_get
    }

    std::cout << "watch_get ...ok" << '\n';
    {
        // begin test watch_get (ok)
        auto msg1 = new common::ProtoMessage;
       //put first 
        msg1->expire_time = getticks() + 1000;
        watchpb::DsKvWatchPutRequest req1;

        req1.mutable_header()->set_range_id(2);
        req1.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req1.mutable_header()->mutable_range_epoch()->set_version(1);

        req1.mutable_req()->mutable_kv()->add_key("01004001");
        req1.mutable_req()->mutable_kv()->set_value("01004001:value");

        auto len1 = req1.ByteSizeLong();
        msg1->body.resize(len1);
        ASSERT_TRUE(req1.SerializeToArray(msg1->body.data(), len1));

        range_server_->WatchPut(msg1);
        //get next
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_version(0);
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        if(resp.resp().events_size()){
            ;
            FLOG_DEBUG("value:%s", resp.resp().events(0).kv().value().c_str());
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01004001:value");
        }

        // end test watch_get
    }

    std::cout << "watch_delete ...key empty" << '\n';
    {
        // begin test watch_get (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("");
        req.mutable_req()->mutable_kv()->set_version(0);
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test watch_get
    }

    std::cout << "watch_get ...no leader" << '\n';
    {
        // begin test watch_get (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 0;
        range_server_->ranges_[1]->setLeaderFlag(false);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test watch_get
    }

    std::cout << "watch_get ...not leader" << '\n';
    {
        // begin test watch_get (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test watch_get
    }

    std::cout << "watch_get ...not in range" << '\n';
    {
        // begin test watch_get (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test watch_get
    }

    std::cout << "watch_get ...retry split range" << '\n';
    {
        // begin test watch_get (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->setLeaderFlag(true);
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->setLeaderFlag(true);
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));
        if(resp.header().has_error()) {
            std::cout << "error:" << resp.header().error().message().c_str() << std::endl;
        }
        //ASSERT_FALSE(resp.header().has_error());
        if(resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");

        // end test watch_get
    }
    


    std::cout << "watch_delete ...key empty" << '\n';
    {
        // begin test watch_delete (key empty)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());
        // end test watch_delete
    }

    std::cout << "watch_delete ...no leader" << '\n';
    {
        // begin test watch_delete (no leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 0;
        range_server_->ranges_[1]->setLeaderFlag(false);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        // end test watch_delete
    }

    std::cout << "watch_delete ...not leader" << '\n';
    {
        // begin test watch_delete (not leader)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        // end test watch_delete
    }

    std::cout << "watch_delete ...not in range" << '\n';
    {
        // begin test watch_delete (not in range)

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        // end test watch_delete
    }

    /*
    std::cout << "watch_get ...ensure not to be deleted" << '\n';
    {
        // begin test watch_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));
       
        if(resp.header().has_error()) {
            std::cout << '\n';
            std::cout << "error:" << resp.header().error().message().c_str() << std::endl;
        }
        EXPECT_FALSE(resp.header().has_error());
        if(resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01003001:value");

        // end test watch_get
    }
*/
    std::cout << "watch_get 2...ensure not to be deleted" << '\n';
    {
        // begin test watch_get( ensure not to be deleted )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        EXPECT_FALSE(resp.header().has_error());
        if(resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value() == "01004001:value");

        // end test watch_get
    }
    

    {
        // begin test watch_delete( ok )

        // set leader
        auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->setLeaderFlag(true);

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // end test watch_delete
    }

    {
        // begin test watch_delete (ok, retry split range)

        // set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->setLeaderFlag(true);
        }

        {
            auto raft = static_cast<RaftMock *>(range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->setLeaderFlag(true);
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsKvWatchDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_kv()->add_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchDel(msg);

        watchpb::DsKvWatchDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        EXPECT_FALSE(resp.header().has_error());

        // end test watch_delete
    }

    {
        // begin test watch_get(ensure watch delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01003001");
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        EXPECT_FALSE(resp.header().has_error());
        if(resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value().empty());

        // end test watch_get
    }

    {
        // begin test watch_get (ensure watch delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        watchpb::DsWatchRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->mutable_kv()->add_key("01004001");
        req.mutable_req()->mutable_kv()->set_version(0);
        req.mutable_req()->set_longpull(now+5000);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->WatchGet(msg);

        watchpb::DsWatchResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        EXPECT_FALSE(resp.header().has_error());
        if(resp.resp().events_size())
            ASSERT_TRUE(resp.resp().events(0).kv().value().empty());

        // end test watch_get
    }
   /* ////////////////////////////////////////////////////////////////////////ignore
    {
        // begin test delete range (range 1)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        // test meta_store
        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        // end test delete range
    }

    {
        // begin test delete range (range 2)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(2);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->find(2) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock *>(context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        // test meta_store
        std::vector<std::string> metas;
        auto ret = range_server_->meta_store_->GetAllRange(metas);
        ASSERT_TRUE(metas.size() == 0) << metas.size();
        // end test delete range
    }
    */
}
