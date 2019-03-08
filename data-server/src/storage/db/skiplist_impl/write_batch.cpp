//
// Created by young on 19-2-14.
//

#include "storage/db/skiplist_impl/write_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MemWriteBatch::MemWriteBatch(SkipListDBImpl* db): db_(db) {

}

Status MemWriteBatch::Put(const std::string &key, const std::string &value) {
    return Status(Status::kOk);
}

Status MemWriteBatch::Put(void* column_family, const std::string& key, const std::string& value) {
    return Status(Status::kOk);
}

Status MemWriteBatch::Delete(const std::string &key) {
    return Status(Status::kOk);
}

Status MemWriteBatch::Delete(void* column_family, const std::string& key) {
    return Status(Status::kOk);
}

}
}
}