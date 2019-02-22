#include <gtest/gtest.h>

#include "base/util.h"
#include "helper/store_test_fixture.h"


int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;
using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;

// test base the account table
class StoreTxnTest : public StoreTestFixture {
public:
    StoreTxnTest() : StoreTestFixture(CreateAccountTable()) {}

protected:
    // inserted rows
    std::vector<std::vector<std::string>> rows_;
};

TEST_F(StoreTxnTest, Prepare) {
}

TEST_F(StoreTxnTest, Decide) {
}

}

