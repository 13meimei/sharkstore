_Pragma("once");

#include "mem_store/iterator.h"
#include "base/status.h"
#include "storage/db/iterator_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Metric;

class MemIterator: public IteratorInterface{
public:
    MemIterator(memstore::Iterator<std::string, std::string>* it);
    ~MemIterator();

    bool Valid();
    void Next();

    Status status();
    std::string key();
    std::string value();

    uint64_t key_size();
    uint64_t value_size();

private:
    memstore::Iterator<std::string, std::string>* rit_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
