#include "iostream"
#include <gtest/gtest.h>
#include "storage/db/multi_v_key.h"
#include "helper/mock/mass_tree_mvcc_mock.h"

using namespace sharkstore::test::mock;

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
    sharkstore::dataserver::storage::DbInterface* db_;
};

TEST_F(MassMvccTest, MassPutGet) {
    auto db = getenv("DB");
    std::cout << db << std::endl;
    ASSERT_STREQ(db, "mass-tree");

    std::string k = "k";
    std::string v = "v";
    db_->Put(k, v);
    db_->Put(k, v);
    db_->Put(k, v);
    db_->Put(k, v+v);

    std::string* retVal = new std::string();
    db_->Get(k, retVal);
    std::cout << *retVal << std::endl;

    ASSERT_STREQ(retVal->c_str(), (v+v).c_str());
}

TEST_F(MassMvccTest, MassDelete) {
    std::string k1 = "k1";
    std::string v1 = "v1";
    std::string k2 = "k2";
    std::string v2 = "v2";
    db_->Put(k1, v1);
    db_->Put(k2, v2);

    db_->Delete(k1);

    auto ite = db_->NewIterator("", "");
    while (ite->Valid()) {
        sharkstore::dataserver::storage::MultiVersionKey mvk(ite->key(), 0, false);
        mvk.from_string(ite->key());
        std::cout << "[MassDelete]" << mvk.key() << "," << mvk.ver() << "," <<  mvk.is_del() << std::endl;
        ASSERT_STREQ(mvk.key().c_str(), k2.c_str());
        ite->Next();
    }
}

TEST_F(MassMvccTest, MassIterator) {
    std::string k1 = "k1";
    std::string v1 = "v1";
    std::string k2 = "k2";
    std::string v2 = "v2";
    std::string k3 = "k3";
    std::string v3 = "v3";
    db_->Put(k1, v1);
    db_->Put(k2, v2);
    db_->Put(k3, v3);

    int i = 0;
    auto ite = db_->NewIterator("", "");
    while (ite->Valid()) {
        sharkstore::dataserver::storage::MultiVersionKey mvk(ite->key(), 0, false);
        mvk.from_string(ite->key());
        std::cout << "[MassIterator]" << mvk.key() << "," << mvk.ver() << "," <<  mvk.is_del() << std::endl;
        switch (i) {
            case 0:
                ASSERT_STREQ(mvk.key().c_str(), k1.c_str());
                break;
            case 1:
                ASSERT_STREQ(mvk.key().c_str(), k2.c_str());
                break;
            case 2:
                ASSERT_STREQ(mvk.key().c_str(), k3.c_str());
                break;
            default:
                break;
        }
        ite->Next();
        ++i;
    }
}

TEST_F(MassMvccTest, MassGc) {
    std::string k1 = "k1";
    std::string v1 = "v1";
    std::string k2 = "k2";
    std::string v2 = "v2";
    std::string k3 = "k3";
    std::string v3 = "v3";
    db_->Put(k1, v1);
    db_->Put(k1, v1+v1);
    db_->Put(k1, v1+v1+v1);
    db_->Put(k1, v1+v1+v1+v1);
    db_->Put(k2, v2);
    db_->Put(k3, v3);

    db_->Scrub();

    //db_->Put(k1, v1+v1+v1+v1+v1);
    auto ite = db_->NewIterator("k1", "");
    while (ite->Valid()) {
        sharkstore::dataserver::storage::MultiVersionKey mvk(ite->key(), 0, false);
        mvk.from_string(ite->key());
        std::cout << "[MassGc]" << mvk.key() << "," << mvk.ver() << "," <<  mvk.is_del() << std::endl;
        std::cout << "[MassGc]" << ite->value() << std::endl;
        //ASSERT_STREQ(mvk.key().c_str(), k2.c_str());
        ite->Next();
    }
}

}


