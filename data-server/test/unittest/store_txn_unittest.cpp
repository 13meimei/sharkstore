#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/util.h"
#include "helper/helper_util.h"
#include "helper/store_test_fixture.h"
#include "helper/request_builder.h"
#include "helper/txn_request_builder.h"
#include "common/ds_encoding.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;
using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;
using namespace txnpb;

// test base the account table
class StoreTxnTest : public StoreTestFixture {
public:
    StoreTxnTest() : StoreTestFixture(CreateAccountTable()) {}

protected:
    // inserted rows
    std::vector<std::vector<std::string>> rows_;
};

void insertIntent(TxnIntent* intent,
                  const std::string& key, const std::string& value) {
    intent->set_typ(INSERT);
    intent->set_key(key);
    intent->set_value(value);
}

void insertIntent(TxnIntent* intent,
                  const std::string& key) {
    intent->set_typ(DELETE);
    intent->set_key(key);
}

static void randomIntent(TxnIntent& intent) {
    intent.set_typ(randomInt() % 2 == 0 ? INSERT : DELETE);
    intent.set_key(randomString(20));
    if (intent.typ() == INSERT) {
        intent.set_value(randomString(100));
    }
}

static void randomTxnValue(TxnValue& value) {
    value.set_txn_id(randomString(10, 20));
    randomIntent(*value.mutable_intent());
    value.set_primary_key(randomString(15));
    value.set_expired_at(calExpireAt(10));
}

TEST_F(StoreTxnTest, TxnValue) {
}

//insert
//select and get version
//prepare taking version
TEST_F(StoreTxnTest, Prepare) {
//    ASSERT_TRUE(!table_->GetColumn("id").name().empty());
//
//    rows_ = {{"44","user4","444"},
//            {"22","user2","222"},
//            {"112222","user13333","133311"},
//             {"33","user3","333"}};
//    uint64_t len = rows_.size();
//    auto s = testInsert(rows_, &len);
//    ASSERT_TRUE(s.ok()) << s.ToString();
//
//    std::vector<uint64_t> vers;
//    // select all rows
//    {
//        decltype(rows_) row0;
//        auto s = testTxnSelect(
//                [](TxnSelectRequestBuilder& b) {
//                    b.AddAllFields();
//                },
//                rows_, vers
//        );
//        ASSERT_TRUE(s.ok()) << s.ToString();
//    }

    txnpb::TxnIntent tnt;
    randomIntent(tnt);
    PrepareRequestBuilder builder;
    auto prepare_req = builder.Build();
    prepare_req.set_txn_id("1");
    std::string pkey("1");
    prepare_req.set_primary_key(pkey);

    PrepareResponse resp;
    uint64_t  version{1};
    store_->TxnPrepare(prepare_req, version, &resp);
    ASSERT_TRUE(resp.errors_size() == 0);
}

TEST_F(StoreTxnTest, Decide) {
}

}

TEST_F(StoreTxnTest, PrepareLocal) {
    PrepareRequestBuilder builder;

    auto req = builder.Build();
    req.set_local(true); // set local

    std::string txn_id("txn_1");
    req.set_txn_id(txn_id);
//    std::string pk("pk_1");
//    req.set_primary_key(pk); // for fill ds intent value

    auto intent = req.add_intents();
    std::string key = randomString(10);
    std::string value = randomString(10);
    insertIntent(intent, key, value);

    PrepareResponse resp;

    uint64_t txn_ver = 1;
    store_->TxnPrepare(req, txn_ver, &resp);
    ASSERT_TRUE(resp.errors_size() == 0);

    // get key
    std::string actual_value;
    auto s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok()) << s.ToString();

    EncodeIntValue(&value, kVersionColumnID, static_cast<int64_t>(txn_ver));
    ASSERT_EQ(actual_value, value);
}

TEST_F(StoreTxnTest, PrepareLocal_intentKeyLocked) {
    std::string key = randomString(10);
    std::string value = randomString(10);
    std::string actual_value;

    PrepareRequestBuilder builder;
    auto req1 = builder.Build();
    auto req2 = builder.Build();
    auto intent1 = req1.add_intents();
    auto intent2 = req2.add_intents();
    PrepareResponse resp1;
    PrepareResponse resp2;

    // txn1 without flag local
    req1.set_txn_id("txn_1");
    insertIntent(intent1, key, value);

    store_->TxnPrepare(req1, 1, &resp1);

    ASSERT_TRUE(resp1.errors_size() == 0);
    store_->Get(key, &actual_value);
    ASSERT_TRUE(actual_value.empty());

    // txn2 with flag local
    req2.set_local(true);

    req2.set_txn_id("txn_2");
    insertIntent(intent2, key, value);

    store_->TxnPrepare(req2, 2, &resp2);

    ASSERT_TRUE((resp2.errors().size() == 1) &&
                (resp2.mutable_errors(0)->err_type() == TxnError_ErrType_LOCKED));
    store_->Get(key, &actual_value);
    ASSERT_TRUE(actual_value.empty());
}

TEST_F(StoreTxnTest, Iterator) {
    // test empty iter
    {
        auto iter = store_->NewTxnIterator("", "");
        std::string key, v1, v2;
        bool over = false;
        auto s = iter->Next(key, v1, v2, over);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(over);
        ASSERT_TRUE(key.empty() && v1.empty() && v2.empty());
    }

    struct Elem {
        std::string key;
        std::string db_value;
        std::string txn_value;
    };

    std::vector<Elem> expected;
    for (int i = 0; i < 100; ++i) {
        char suffix[20] = {'\0'};
        snprintf(suffix, 20, "%05d", i);
        std::string key;
        EncodeKeyPrefix(&key, meta_.table_id());
        key += suffix;

        expected.emplace_back(Elem());
        expected.back().key = key;
        int choice = randomInt() % 3;
        bool has_value = choice == 0 || choice == 2;
        bool has_intent = choice == 1 || choice == 2;
        ASSERT_TRUE(has_value || has_intent);
        if (has_value) {
            std::string value = randomString(10, 20);
            auto s = store_->Put(key, value);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().db_value = value;
        }
        if (has_intent) {
            TxnValue txn_val;
            randomTxnValue(txn_val);
            txn_val.mutable_intent()->set_key(key);
            auto s = putTxn(key, txn_val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().txn_value = txn_val.SerializeAsString();
        }
    }

    int index = 0;
    auto iter = store_->NewTxnIterator("", "");
    ASSERT_TRUE(iter != nullptr);
    bool over = false;
    while (!over) {
        std::string key, db_value, txn_value;
        auto s = iter->Next(key, db_value, txn_value, over);
        ASSERT_TRUE(s.ok()) << s.ToString();
        if (over) break;
        ASSERT_EQ(key, expected[index].key);
        ASSERT_EQ(db_value, expected[index].db_value);
        ASSERT_EQ(txn_value, expected[index].txn_value);
        ++index;
    }
    ASSERT_EQ(index, expected.size());
}


TEST_F(StoreTxnTest, Scan) {
    // scan empty
    {
        txnpb::ScanRequest req;
        txnpb::ScanResponse resp;
        auto s = store_->TxnScan(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(resp.kvs_size(), 0);
        ASSERT_EQ(resp.code(), 0);
    }

    // insert some keys
    struct Elem {
        std::string key;
        std::string db_value;
        std::string txn_value;
    };
    std::vector<Elem> expected;
    for (int i = 0; i < 100; ++i) {
        char suffix[20] = {'\0'};
        snprintf(suffix, 20, "%05d", i);
        std::string key;
        EncodeKeyPrefix(&key, meta_.table_id());
        key += suffix;

        expected.emplace_back(Elem());
        expected.back().key = key;
        int choice = randomInt() % 3;
        bool has_value = choice == 0 || choice == 2;
        bool has_intent = choice == 1 || choice == 2;
        ASSERT_TRUE(has_value || has_intent);
        if (has_value) {
            std::string value = randomString(10, 20);
            auto s = store_->Put(key, value);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().db_value = value;
        }
        if (has_intent) {
            TxnValue txn_val;
            randomTxnValue(txn_val);
            txn_val.mutable_intent()->set_key(key);
            auto s = putTxn(key, txn_val);
            ASSERT_TRUE(s.ok()) << s.ToString();
            expected.back().txn_value = txn_val.SerializeAsString();
        }
    }

    // scan all
    {
        txnpb::ScanRequest req;
        req.set_max_count(1000000);
        txnpb::ScanResponse resp;
        auto s = store_->TxnScan(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(resp.kvs_size(), expected.size());
        for (int i = 0; i < resp.kvs_size(); ++i) {
            ASSERT_EQ(resp.kvs(i).key(), expected[i].key);
            ASSERT_EQ(resp.kvs(i).value(), expected[i].db_value);
            ASSERT_EQ(resp.kvs(i).has_intent(), !expected[i].txn_value.empty());
            if (resp.kvs(i).has_intent()) {
                txnpb::TxnValue txn_value;
                ASSERT_TRUE(txn_value.ParseFromString(expected[i].txn_value));
                auto& intent = resp.kvs(i).intent();
                ASSERT_EQ(intent.op_type(), txn_value.intent().typ());
                ASSERT_EQ(intent.txn_id(), txn_value.txn_id());
                ASSERT_EQ(intent.primary_key(), txn_value.primary_key());
                ASSERT_EQ(intent.value(), txn_value.intent().value());
            }
        }
    }
}
