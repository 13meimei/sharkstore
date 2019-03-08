#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/util.h"
#include "helper/store_test_fixture.h"
#include "helper/request_builder.h"
#include "../helper/txn_request_builder.h"

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
    ASSERT_TRUE(!table_->GetColumn("id").name().empty());

    rows_ = {{"44","user4","444"},
            {"22","user2","222"},
            {"112222","user13333","133311"},
             {"33","user3","333"}};
    uint64_t len = rows_.size();
    auto s = testInsert(rows_, &len);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<uint64_t> vers;
    // select all rows
    {
        decltype(rows_) row0;
        auto s = testTxnSelect(
                [](TxnSelectRequestBuilder& b) {
                    b.AddAllFields();
                },
                rows_, vers
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

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

