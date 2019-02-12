_Pragma("once");

#include "base/status.h"
#include "storage/iterator_interface.h"
#include <mem_store/iterator.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

class Metric;

class MemIterator: public IteratorInterface{
public:
    MemIterator(memstore::Iterator<std::string, std::string>* it,
                const std::string& start, const std::string& limit);
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
    const std::string limit_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
