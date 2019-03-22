_Pragma("once");

#include "storage/db/iterator_interface.h"
#include "storage/db/multi_v_key.h"
#include "scaner.h"
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class MassTreeDBImpl;
class MassTreeIterator: public IteratorInterface {
public:
    MassTreeIterator() = delete;
    ~MassTreeIterator();

    MassTreeIterator(TreeType* tree, const std::string start, const std::string limit,
        MassTreeDBImpl *db);

    bool Valid();
    void Next();
    Status status();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    Scaner scaner_;
    MassTreeDBImpl *db_;
    MultiVersionKey cur_key_;
    uint64_t ver_;
};

}
}
}
