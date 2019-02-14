//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_DB_INTERFACE_H
#define SHARKSTORE_DS_DB_INTERFACE_H

#include "base/status.h"
#include "write_batch_interface.h"
#include "iterator_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class DbInterface {
public:
    DbInterface() = default;
    virtual ~DbInterface() = default;

    virtual Status Get(const std::string& key, std::string* value) = 0;
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Write(WriteBatchInterface* batch) = 0;
    virtual Status Delete(const std::string& batch) = 0;
    virtual Status DeleteRange(void* column_family,
                               const std::string& begin_key, const std::string& end_key) = 0;
    virtual void* DefaultColumnFamily() = 0;
    virtual IteratorInterface* NewIterator(const std::string& start, const std::string& limit) = 0;
};

}
}
}

#endif //SHARKSTORE_DS_DB_INTERFACE_H
