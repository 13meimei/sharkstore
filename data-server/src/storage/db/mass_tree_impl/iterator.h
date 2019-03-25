_Pragma("once");

#include <memory>
#include "storage/db/iterator_interface.h"
#include "storage/db/multi_v_key.h"
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class Scaner;
class MvccMassTree;

class MassTreeIterator: public IteratorInterface {
public:
    MassTreeIterator() = delete;
    ~MassTreeIterator();

    MassTreeIterator(MvccMassTree *db, std::unique_ptr<Scaner> scaner);

    bool Valid();
    void Next();
    Status status();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    MvccMassTree* db_ = nullptr;
    std::unique_ptr<Scaner> scaner_;
    MultiVersionKey cur_key_;
    uint64_t ver_;
};

}
}
}
