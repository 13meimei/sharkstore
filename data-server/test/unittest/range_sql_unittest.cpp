#include <gtest/gtest.h>

#include "helper/range_test_fixture.h"
#include "helper/helper_util.h"
#include "helper/request_builder.h"
#include "helper/query_parser.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

TEST_F(RangeTestFixture, NoLeader) {
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
    {
        DsDeleteRequest req;
        MakeHeader((req.mutable_header()));
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_FALSE(resp.header().error().not_leader().has_leader());
    }
}

TEST_F(RangeTestFixture, NotLeader) {
    SetLeader(2);
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
    {
        DsDeleteRequest req;
        MakeHeader((req.mutable_header()));
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_not_leader());
        ASSERT_EQ(resp.header().error().not_leader().range_id(), GetRangeID());
        ASSERT_TRUE(resp.header().error().not_leader().has_leader());
        ASSERT_EQ(resp.header().error().not_leader().leader().node_id(), 2);
        ASSERT_EQ(resp.header().error().not_leader().leader().id(), GetPeerID(2));
    }
}

TEST_F(RangeTestFixture, StaleEpoch) {
    SetLeader(GetNodeID());
    auto old_ver = range_->options().range_epoch().version();
    auto s = Split();
    ASSERT_TRUE(s.ok()) << s.ToString();
    auto old_meta = range_->options();
    auto split_range = context_->FindRange(GetSplitRangeID());
    ASSERT_TRUE(split_range != nullptr);
    auto new_meta = split_range->options();
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header(), old_ver);
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(resp.header().has_error());
        ASSERT_TRUE(resp.header().error().has_stale_epoch());
        auto &stale_err = resp.header().error().stale_epoch();
        ASSERT_EQ(old_meta.ShortDebugString(), stale_err.old_range().ShortDebugString());
        ASSERT_EQ(new_meta.ShortDebugString(), stale_err.new_range().ShortDebugString());
    }
}

TEST_F(RangeTestFixture, CURD) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };

    // insert some rows
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_req()->CopyFrom(builder.Build());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), rows.size());
    }
    // test select
    {

        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();

        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // test delete one
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        builder.AddMatch("id", kvrpcpb::Equal, "1");
        *req.mutable_req() = builder.Build();
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), 1);
    }
    // select and check
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match({rows[1], rows[2]});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // delete all
    {
        DsDeleteRequest req;
        MakeHeader(req.mutable_header());
        DeleteRequestBuilder builder(table_.get());
        *req.mutable_req() = builder.Build();
        DsDeleteResponse resp;
        auto s = TestDelete(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), 2);
    }
    // select and check
    {
        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        *req.mutable_req() = builder.Build();
        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match({});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(RangeTestFixture, MatchExt) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows1 = {
            {"1", "user1", "111"},
    };
    std::vector<std::vector<std::string>> rows2 = {
            {"2", "user2", "222"},
    };
    std::vector<std::vector<std::string>> rows3 = {
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows12 = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
    };
    std::vector<std::vector<std::string>> rows13 = {
            {"1", "user1", "111"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows23 = {
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows0;
    rows0.clear();

    // insert some rows
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_req()->CopyFrom(builder.Build());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), rows.size());
    }

    // test select
    {
        //printf("test select with match_ext\n");

        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();

        //复合条件 not support math operaton in relation express
        //where id = 1 or id > 1 and id = 10 - 9
        //or
        //  =
        //      id
        //      1
        //  and
        //      >
        //          id
        //          1
        //      =
        //          id
        //          -
        //               10
        //               9
        //
        //id = 1
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();

        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //id = 1 or id = 2
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("id", "2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser12(req.req(), resp.resp());
        s = parser12.Match(rows12);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //id = 2 and balance = "222" and name = "user2"
        builder.AppendMatchExt("id", "2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicAnd);
        builder.AppendMatchExt("balance", "222", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicAnd);
        builder.AppendMatchExt("name", "user2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser2(req.req(), resp.resp());
        s = parser2.Match(rows2);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //name = user33 or name = "user3" => rows3
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user33", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("name", "user3", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser3(req.req(), resp.resp());
        s = parser3.Match(rows3);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //id = 2 or (name = "user2" and balance = "100") => rows2
        //builder.AppendMatchExt("id", "2", ::kvrpcpb::E_NotEqual, ::kvrpcpb::E_LogicOr);
        //builder.AppendMatchExt("name", "user2", ::kvrpcpb::E_Larger, ::kvrpcpb::E_LogicAnd);
        //builder.AppendMatchExt("balance", "100", ::kvrpcpb::E_Less, ::kvrpcpb::E_Invalid);

        //id <> 1 or id > 1 and (id < 4 and id > 1) => rows23
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_NotEqual, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_Larger, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("id", "4", ::kvrpcpb::E_Less, ::kvrpcpb::E_LogicAnd);
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_Larger, ::kvrpcpb::E_Invalid);
        //builder.AppendMatchExt("name", "user_1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicAnd);
        //builder.AppendMatchExt("balance", "100", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser33(req.req(), resp.resp());
        s = parser33.Match(rows23);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //id = 2 or name = "user2" and balance = "100" => rows0
        builder.AppendMatchExt("id", "1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicAnd);
        builder.AppendMatchExt("name", "user_1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendMatchExt("balance", "111", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser1_1(req.req(), resp.resp());
        s = parser1_1.Match(rows1);
    }
}

TEST_F(RangeTestFixture, MatchMathExpr) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows0;
    std::vector<std::vector<std::string>> rows2 = {
            {"2", "user2", "222"},
    };

    std::vector<std::vector<std::string>> rows1 = {
            {"1", "user1", "111"},
    };

    // insert some rows
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_req()->CopyFrom(builder.Build());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), rows.size());
    }
    // test select
    {

        DsSelectRequest req;
        MakeHeader(req.mutable_header());
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();

        builder.ClearMatchExt();
        //id = 1 + 1
        builder.AppendCompCond("id", "1+1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();

        DsSelectResponse resp;
        auto s = TestSelect(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        SelectResultParser parser(req.req(), resp.resp());
        s = parser.Match(rows2);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //where id = 10 - 9
        builder.AppendCompCond("id", "10-9", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser1_1(req.req(), resp.resp());
        s = parser1_1.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //where id = 1*1
        builder.AppendCompCond("id", "1*1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser1(req.req(), resp.resp());
        s = parser1.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //where id = 10/9
        builder.AppendCompCond("id", "10\/10", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser2(req.req(), resp.resp());
        s = parser2.Match(rows1);
        ASSERT_TRUE(s.ok()) << s.ToString();

        builder.AddAllFields();
        builder.ClearMatchExt();
        //TO DO not support column expression id = id-1
        //just test nest expression: where id = id+2 or id = id-1 or id = id*2 or id = id/1 
        builder.AppendCompCond("id", "id+2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
        builder.AppendCompCond("id", "id-1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        //builder.AppendCompCond("id", "id*2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        //builder.AppendCompCond("id", "id\/1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
        *req.mutable_req() = builder.Build();
        s = TestSelect(req, &resp);
        SelectResultParser parser3(req.req(), resp.resp());
        s = parser3.Match(rows0);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

}

TEST_F(RangeTestFixture, DISABLED_MatchMathExprDaemon) {
    SetLeader(GetNodeID());

    std::vector<std::vector<std::string>> rows = {
            {"1", "user1", "111"},
            {"2", "user2", "222"},
            {"3", "user3", "333"},
    };
    std::vector<std::vector<std::string>> rows0;
    std::vector<std::vector<std::string>> rows2 = {
            {"2", "user2", "222"},
    };

    std::vector<std::vector<std::string>> rows1 = {
            {"1", "user1", "111"},
    };

    // insert some rows
    {
        DsInsertRequest req;
        MakeHeader(req.mutable_header());
        InsertRequestBuilder builder(table_.get());
        builder.AddRows(rows);
        req.mutable_req()->CopyFrom(builder.Build());
        DsInsertResponse resp;
        auto s = TestInsert(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
        ASSERT_EQ(resp.resp().code(), 0);
        ASSERT_EQ(resp.resp().affected_keys(), rows.size());
    }

    int cnt{1000000};
    do {
        // test select
        {

            DsSelectRequest req;
            MakeHeader(req.mutable_header());
            SelectRequestBuilder builder(table_.get());
            builder.AddAllFields();

            builder.ClearMatchExt();
            //id = 1 + 1
            builder.AppendCompCond("id", "1+1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            *req.mutable_req() = builder.Build();

            DsSelectResponse resp;
            auto s = TestSelect(req, &resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_FALSE(resp.header().has_error()) << resp.header().error().ShortDebugString();
            SelectResultParser parser(req.req(), resp.resp());
            s = parser.Match(rows2);
            ASSERT_TRUE(s.ok()) << s.ToString();

            builder.AddAllFields();
            builder.ClearMatchExt();
            //where id = 10 - 9
            builder.AppendCompCond("id", "10-9", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            *req.mutable_req() = builder.Build();
            s = TestSelect(req, &resp);
            SelectResultParser parser1_1(req.req(), resp.resp());
            s = parser1_1.Match(rows1);
            ASSERT_TRUE(s.ok()) << s.ToString();

            builder.AddAllFields();
            builder.ClearMatchExt();
            //where id = 1*1
            builder.AppendCompCond("id", "1*1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            *req.mutable_req() = builder.Build();
            s = TestSelect(req, &resp);
            SelectResultParser parser1(req.req(), resp.resp());
            s = parser1.Match(rows1);
            ASSERT_TRUE(s.ok()) << s.ToString();

            builder.AddAllFields();
            builder.ClearMatchExt();
            //where id = 10/9
            builder.AppendCompCond("id", "10\/10", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            *req.mutable_req() = builder.Build();
            s = TestSelect(req, &resp);
            SelectResultParser parser2(req.req(), resp.resp());
            s = parser2.Match(rows1);
            ASSERT_TRUE(s.ok()) << s.ToString();

            builder.AddAllFields();
            builder.ClearMatchExt();
            //TO DO not support column expression id = id-1
            //just test nest expression: where id = id+2 or id = id-1 or id = id*2 or id = id/1 
            builder.AppendCompCond("id", "id+2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_LogicOr);
            builder.AppendCompCond("id", "id-1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            //builder.AppendCompCond("id", "id*2", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            //builder.AppendCompCond("id", "id\/1", ::kvrpcpb::E_Equal, ::kvrpcpb::E_Invalid);
            *req.mutable_req() = builder.Build();
            s = TestSelect(req, &resp);
            SelectResultParser parser3(req.req(), resp.resp());
            s = parser3.Match(rows0);
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

    } while (cnt-- > 0);

}
}
