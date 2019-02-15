_Pragma("once");

#include <rocksdb/db.h>
#include "base/status.h"
#include "storage/iterator_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Metric;

class RocksIterator: public IteratorInterface{
public:
    RocksIterator(rocksdb::Iterator* it, const std::string& start,
             const std::string& limit);
    ~RocksIterator();

    bool Valid();
    void Next();

    Status status();
    std::string key();
    std::string value();

    uint64_t key_size();
    uint64_t value_size();

private:
    rocksdb::Iterator* rit_ = nullptr;
    const std::string limit_;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
