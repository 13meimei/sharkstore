//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_WRITE_BATCH_H
#define SHARKSTORE_DS_WRITE_BATCH_H

#include "storage/write_batch_interface.h"
#include "storage/mem_store/store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class WriteBatch;
class MemWriteBatch : public WriteBatchInterface {
public:
    MemWriteBatch() = delete;
    MemWriteBatch(MemStore* db);
    ~MemWriteBatch() = default;

    Status Put(const std::string &key, const std::string &value);
    Status Put(void* column_family, const std::string& key, const std::string& value);
    Status Delete(const std::string &key);
    Status Delete(void* column_family, const std::string& key);

private:
//    WriteBatch batch_;
    MemStore* db_;
};

class WriteBatch {
public:
    WriteBatch() = default;
    WriteBatch(const WriteBatch& batch) = delete;
    WriteBatch& operator=(const WriteBatch& batch) = delete;
    ~WriteBatch() = default;

private:
    std::vector<std::string, std::string> batch_;
};

}}}

#endif //SHARKSTORE_DS_WRITE_BATCH_H
