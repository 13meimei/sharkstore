//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_DB_INTERFACE_H
#define SHARKSTORE_DS_DB_INTERFACE_H

#include <memory>
#include <unordered_map>
#include "base/status.h"
#include "iterator_interface.h"
#include "write_batch_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Store;
class DbInterface {
public:
    DbInterface() = default;
    virtual ~DbInterface() = default;

    DbInterface(const DbInterface&) = delete;
    DbInterface& operator=(const DbInterface&) = delete;

    virtual bool IsInMemory() = 0;

    virtual Status Open() = 0;

    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Get(void* column_family,
                       const std::string& key, std::string* value) = 0;

    virtual Status Put(void* column_family,
                       const std::string& key, const std::string& value) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;

    virtual std::unique_ptr<WriteBatchInterface> NewBatch() = 0;
    virtual Status Write(WriteBatchInterface* batch) = 0;

    virtual Status Delete(const std::string& key) = 0;
    virtual Status Delete(void* column_family, const std::string& key) = 0;

    virtual Status DeleteRange(void* column_family,
                               const std::string& begin_key, const std::string& end_key) = 0;

    virtual void* DefaultColumnFamily() = 0;
    virtual void* TxnCFHandle() = 0;

    virtual IteratorInterface* NewIterator(const std::string& start, const std::string& limit) = 0;
    virtual Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                                std::unique_ptr<IteratorInterface>& txn_iter,
                                const std::string& start, const std::string& limit) = 0;

    virtual void GetProperty(const std::string& k, std::string* v) = 0;
    virtual Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) = 0;
    virtual Status SetOptions(void* column_family,
            const std::unordered_map<std::string, std::string>& new_options) = 0;

    virtual std::string GetMetrics() = 0;
};

}
}
}

#endif //SHARKSTORE_DS_DB_INTERFACE_H
