#include "range_test_fixture.h"

#include "storage/meta_store.h"
#include "base/util.h"
#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace sharkstore::dataserver;

RangeTestFixture::RangeTestFixture(std::unique_ptr<Table> t) {
}

void RangeTestFixture::SetUp() {
    if (!table_) {
        throw std::runtime_error("invalid table");
    }

    initContext();

    auto meta = MakeRangeMeta(table_.get());
//    range_.reset(new range::Range(&context_, meta));
}

void RangeTestFixture::initContext() {
 //   context_.node_id = 1;

    char path[] = "/tmp/sharkstore_ds_range_test_XXXXXX";
    char* tmp = mkdtemp(path);
    ASSERT_TRUE(tmp != NULL);
    tmp_dir_ = tmp;

    // open data rocksdb
    {
        rocksdb::Options ops;
        ops.create_if_missing = true;
        ops.error_if_exists = true;
  //      auto s = rocksdb::DB::Open(ops, JoinFilePath({tmp_dir_, "data"}), &context_.rocks_db);
  //      ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // open meta store
    {
   //     context_.meta_store = new storage::MetaStore(JoinFilePath({tmp_dir_, "meta"}));
   //     auto s = context_.meta_store->Open();
   //     ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

void RangeTestFixture::destroyContext() {
    RemoveDirAll(tmp_dir_.c_str());
}

void RangeTestFixture::TearDown() {
    destroyContext();
}




} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
