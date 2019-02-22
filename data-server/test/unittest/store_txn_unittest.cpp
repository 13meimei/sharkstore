#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/util.h"
#include "helper/store_test_fixture.h"

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

TEST_F(StoreTxnTest, Prepare) {
}

TEST_F(StoreTxnTest, Decide) {
}

}

