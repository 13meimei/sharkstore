//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_DB_INTERFACE_H
#define SHARKSTORE_DS_DB_INTERFACE_H

#include "base/status.h"
#include "write_batch_interface.h"
#include "iterator_interface.h"
#include "write_batch_interface.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Store;
class DbInterface {
public:
    DbInterface() = default;
    virtual ~DbInterface() = default;

    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Get(void* column_family,
                       const std::string& key, void* value) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Write(WriteBatchInterface* batch) = 0;
    virtual Status Delete(const std::string& key) = 0;
    virtual Status DeleteRange(void* column_family,
                               const std::string& begin_key, const std::string& end_key) = 0;
    virtual void* DefaultColumnFamily() = 0;
    virtual IteratorInterface* NewIterator(const std::string& start, const std::string& limit) = 0;
    virtual void GetProperty(const std::string& k, std::string* v) = 0;

    virtual Status SetOptions(void* column_family,
                              const std::unordered_map<std::string, std::string>& new_options) = 0;
    virtual Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) = 0;
    virtual Status CompactRange(void* options, void* begin, void* end) = 0;
    virtual Status Flush(void* fops) = 0;

public:
    virtual Status Insert(storage::Store* store,
                          const kvrpcpb::InsertRequest& req, uint64_t* affected) = 0;
    virtual WriteBatchInterface* NewBatch() = 0;
};

}
}
}

#endif //SHARKSTORE_DS_DB_INTERFACE_H
