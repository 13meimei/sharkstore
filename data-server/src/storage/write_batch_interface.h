//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_WRITE_BATCH_INTERFACE_H
#define SHARKSTORE_DS_WRITE_BATCH_INTERFACE_H

#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class WriteBatchInterface {
public:
    WriteBatchInterface() = default;
    virtual ~WriteBatchInterface() = default;

    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Delete(const std::string& key) = 0;
};

}
}
}

#endif //SHARKSTORE_DS_WRITE_BATCH_INTERFACE_H
