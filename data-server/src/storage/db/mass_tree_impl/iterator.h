_Pragma("once");

#include "storage/db/iterator_interface.h"
#include "mass_tree_impl.h"
#include "scaner.h"
#include "base/status.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class MassTreeIterator: public IteratorInterface {
public:
    MassTreeIterator() = delete;
    ~MassTreeIterator() = default;

    MassTreeIterator(TreeType* tree, const std::string start, const std::string limit):
            scaner_(tree, start, limit) {

    }

    bool Valid();
    void Next();
    Status status();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    Scaner scaner_;
};

}
}
}
