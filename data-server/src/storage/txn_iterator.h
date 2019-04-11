_Pragma("once");

#include <proto/gen/txn.pb.h>
#include "db/iterator_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// merge data and txn iterator
class TxnIterator {
public:
    TxnIterator(std::unique_ptr<IteratorInterface> data_iter,
        std::unique_ptr<IteratorInterface> txn_iter);

    Status Next(std::string& key, std::string& data_value, std::string& intent_value, bool &over);

private:
    bool valid();

private:
    std::unique_ptr<IteratorInterface> data_iter_;
    std::unique_ptr<IteratorInterface> txn_iter_;
    bool over_ = false;
    Status status_;
};

} // namespace storage
} // namespace dataserver
} // namespace sharkstore
