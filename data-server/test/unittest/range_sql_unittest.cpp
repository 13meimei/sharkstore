#include <gtest/gtest.h>
#include <fastcommon/shared_func.h>

#include "helper/cpp_permission.h"

#include "frame/sf_util.h"
#include "base/status.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "range/range.h"
#include "storage/store.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "proto/gen/schpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "helper/table.h"
#include "helper/helper_util.h"
#include "helper/query_builder.h"
#include "helper/mock/socket_session_mock.h"
#include "helper/mock/raft_server_mock.h"
#include "helper/range_test_fixture.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

char level[8] = "debug";

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class RangeSQLTest: public ::testing::Test {
protected:
    void SetUp() override {
        log_init2();
        set_log_level(level);

        strcpy(ds_config.rocksdb_config.path, "/tmp/sharkstore_ds_store_test_");
        strcat(ds_config.rocksdb_config.path, std::to_string(getticks()).c_str());
        ds_config.range_config.recover_concurrency = 1;

        range_server_ = new server::RangeServer;

        context_ = new server::ContextServer;

        context_->node_id           = 1;
        context_->range_server      = range_server_;
        context_->socket_session    = new SocketSessionMock;
        context_->raft_server = new RaftServerMock;
        context_->run_status = new server::RunStatus;

        range_server_->Init(context_);

        table_ = CreateAccountTable();
    }

    void TearDown() override {
        DestroyDB(ds_config.rocksdb_config.path, rocksdb::Options());

        delete context_->range_server;
        delete context_->socket_session;
        delete context_->raft_server;
        delete context_->run_status;
        delete context_;
    }

    common::ProtoMessage* CreateInsertMessage() {
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;

        kvrpcpb::DsInsertRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        InsertRequestBuilder builder(table_.get());
        builder.AddRow({"1", "name1", "1"});
        builder.AddRow({"2", "name2", "2"});
        auto insert_req = builder.Build();
        req.mutable_req()->Swap(&insert_req);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        req.SerializeToArray(msg->body.data(), len);

        return msg;
    }

    common::ProtoMessage* CreateSelectMessage() {
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;

        kvrpcpb::DsSelectRequest req;
        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        auto select_req = builder.Build();
        req.mutable_req()->Swap(&select_req);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        req.SerializeToArray(msg->body.data(), len);

        return msg;
    }

protected:
    std::unique_ptr<Table> table_;
    server::ContextServer   *context_;
    server::RangeServer     *range_server_;
};

TEST_F(RangeTestFixture, Test) {
    range_->is_leader_ = true;
}

TEST_F(RangeSQLTest, Test) {
    {
        //begin test create range
        auto msg = new common::ProtoMessage;
        schpb::CreateRangeRequest req;
        auto meta = MakeRangeMeta(table_.get());
        req.mutable_range()->Swap(&meta);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->CreateRange(msg);
        ASSERT_FALSE(range_server_->ranges_.empty());

        ASSERT_TRUE(range_server_->Find(1) != nullptr);

        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        //end test create range
    }

    {
        //begin test insert (no leader)
        auto msg = CreateInsertMessage();
        range_server_->Insert(msg);

        kvrpcpb::DsInsertResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");
    }

    {
        //begin test insert (not leader)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->is_leader_ = false;

        auto msg = CreateInsertMessage();

        range_server_->Insert(msg);

        kvrpcpb::DsInsertResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);
    }

    {
        //begin test insert (stale epoch)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader_ = true;

        // set higher version
        // TODO:
        // range_server_->ranges_[1]->meta_.mutable_range_epoch()->set_version(2);

        auto msg = CreateInsertMessage();

        range_server_->Insert(msg);

        kvrpcpb::DsKvRawPutResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        // rollback version
        // TODO:
        // range_server_->ranges_[1]->meta_.mutable_range_epoch()->set_version(1);

        //end test insert
    }

    {
        //begin test insert (ok)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = CreateInsertMessage();

        range_server_->Insert(msg);

        kvrpcpb::DsInsertResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().affected_keys() == 2) << resp.resp().affected_keys();

        //end test insert
    }

    {
        auto msg = CreateSelectMessage();

        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().rows_size() == 2) << "code:" << resp.resp().code() << " size:"  << resp.resp().rows_size();

        //end test select
    }

    {
        //begin test select (no leader)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 0;
        range_server_->ranges_[1]->is_leader_ = false;

        auto msg = CreateSelectMessage();
        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        //end test select
    }

    {
        //begin test select (not leader)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = CreateSelectMessage();
        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        //end test select
    }

    {
        //begin test select (not in range)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader_ = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        //end test select
    }

    {
        //begin test select (key stale epoch)

        //set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->is_leader_ = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsSelectRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01005001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        //end test select
    }

    {
        //begin test select(scope query stale epoch)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsSelectRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->mutable_scope()->set_start("01003");
        req.mutable_req()->mutable_scope()->set_limit("01004");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());

        //end test select
    }

    {
        //begin test select (ok, retry split range)

        //set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->is_leader_ = true;
        }

        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->is_leader_ = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsSelectRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->Select(msg);

        kvrpcpb::DsSelectResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().rows_size() == 1);
        ASSERT_TRUE(resp.resp().rows(0).key() == "01004001");

        //end test select
    }
/*
    {
        //begin test delete (no leader)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 0;
        range_server_->ranges_[1]->is_leader = false;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().message() == "no leader");

        //end test delete
    }

    {
        //begin test delete (not leader)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 2;
        range_server_->ranges_[1]->is_leader = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_TRUE(resp.header().error().not_leader().leader().node_id() == 2);

        //end test delete
    }

    {
        //begin test delete (not in range)

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader = true;

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_key_not_in_range());

        //end test delete
    }

    {
        //begin test select( ensure not to be deleted )

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader = true;


        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01003001:value");

        //end test select
    }

    {
        //begin test select( ensure not to be deleted )

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[2]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader = true;


        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value() == "01004001:value");

        //end test select
    }

    {
        //begin test delete( ok )

        //set leader
        auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
        raft->ops_.leader = 1;
        range_server_->ranges_[1]->is_leader = true;


        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        //end test delete
    }

    {
        //begin test delete (ok, retry split range)

        //set leader
        range_server_->ranges_[1]->split_range_id_ = 2;
        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[1]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[1]->is_leader = true;
        }

        {
            auto raft = static_cast<RaftMock*> (range_server_->ranges_[2]->raft_.get());
            raft->ops_.leader = 1;
            range_server_->ranges_[2]->is_leader = true;
        }

        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawDeleteRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(2);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawDelete(msg);

        kvrpcpb::DsKvRawDeleteResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        //end test delete
    }

    {
        //begin test select(ensure raw delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(1);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01003001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        //end test select
    }

    {
        //begin test select (ensure raw delete)
        auto msg = new common::ProtoMessage;
        msg->expire_time = getticks() + 1000;
        kvrpcpb::DsKvRawGetRequest req;

        req.mutable_header()->set_range_id(2);
        req.mutable_header()->mutable_range_epoch()->set_conf_ver(1);
        req.mutable_header()->mutable_range_epoch()->set_version(1);

        req.mutable_req()->set_key("01004001");

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->RawGet(msg);

        kvrpcpb::DsKvRawGetResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());
        ASSERT_TRUE(resp.resp().value().empty());

        //end test select
    }
*/
    {
        //begin test delete range (range 1)
        auto msg = new common::ProtoMessage;
        schpb::DeleteRangeRequest req;
        req.set_range_id(1);

        auto len = req.ByteSizeLong();
        msg->body.resize(len);
        ASSERT_TRUE(req.SerializeToArray(msg->body.data(), len));

        range_server_->DeleteRange(msg);

        ASSERT_TRUE(range_server_->Find(1) == nullptr);

        schpb::DeleteRangeResponse resp;
        auto session_mock = static_cast<SocketSessionMock*> (context_->socket_session);
        ASSERT_TRUE(session_mock->GetResult(&resp));

        ASSERT_FALSE(resp.header().has_error());

        //test meta_store
        std::vector<metapb::Range> metas;
        auto ret = range_server_->meta_store_->GetAllRange(&metas);

        ASSERT_TRUE(metas.size() == 1) << metas.size();
        //end test delete range
    }
}

