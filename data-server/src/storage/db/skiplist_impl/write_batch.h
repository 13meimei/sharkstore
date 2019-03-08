//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_WRITE_BATCH_H
#define SHARKSTORE_DS_WRITE_BATCH_H

#include "storage/db/write_batch_interface.h"
#include "skiplist_impl.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class SkipListDBImpl;

// TODO: limit max batch size
class SkipListWriteBatch : public WriteBatchInterface {
public:
    SkipListWriteBatch() = default;
    ~SkipListWriteBatch() = default;

    Status Put(const std::string &key, const std::string &value) override;
    Status Put(void* column_family, const std::string& key, const std::string& value) override;
    Status Delete(const std::string &key) override;
    Status Delete(void* column_family, const std::string& key) override;

    Status WriteTo(SkipListDBImpl* db);

private:
    enum class EntryType {
        kPut,
        kDelete,
    };

    struct BatchEntry {
        void *cf = nullptr;
        EntryType type = EntryType::kPut;
        std::string key;
        std::string value;
    };

private:
    std::vector<std::unique_ptr<BatchEntry>> entries_;
};

}
}
}

#endif //SHARKSTORE_DS_WRITE_BATCH_H
