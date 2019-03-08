//
// Created by young on 19-2-12.
//

#ifndef SHARKSTORE_DS_ITERATOR_INTERFACE_H
#define SHARKSTORE_DS_ITERATOR_INTERFACE_H

#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class IteratorInterface {
public:
    IteratorInterface() = default;

    virtual ~IteratorInterface() = default;

    virtual bool Valid() = 0;

    virtual void Next() = 0;

    virtual Status status() = 0;

    virtual std::string key() = 0;

    virtual std::string value() = 0;

    virtual uint64_t key_size() = 0;

    virtual uint64_t value_size() = 0;
};

} //namespace sharkstore
} //namespace dataserver
} //namespace storage

#endif //SHARKSTORE_DS_ITERATOR_INTERFACE_H
