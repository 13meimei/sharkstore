#include "iostream"
#include <gtest/gtest.h>
#include "storage/db/multi_v_key.h"
#include "helper/mock/mass_tree_mvcc_mock.h"
#include "helper/mock/mass_tree_iterator_mock.h"

using namespace sharkstore::test::mock;
using namespace sharkstore::dataserver::storage;

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
class MassMvccTest : public testing::Test {
protected:
    MassMvccTest() = default;

    void SetUp() override {
        setenv("DB", "mass-tree-mock", 1);
        db_ = new MvccMassTreeMock();
    }
    void TearDown() override {

    }

protected:
    MvccMassTreeMock* db_;
};

TEST_F(MassMvccTest, SingleKey) {
    auto db = getenv("DB");
    ASSERT_STREQ(db, "mass-tree-mock");

    MultiVersionKey mulitKey;
    std::string k = "a";
    std::string v = "a";
    db_->Put(k, v);

    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));
    ASSERT_TRUE(it->Valid());
    mulitKey = it->getMultiKey();
    //std::cout << mulitKey.key() << " ver=" << mulitKey.ver() << "::::" <<it->key() << std::endl;
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_EQ(mulitKey.ver(), 1);

    db_->Delete(k);

    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));
    ASSERT_TRUE(it->Valid());

    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_TRUE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 2);

    it->Traverse();
    ASSERT_TRUE(it->Valid());
    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_FALSE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 1);
}

}


