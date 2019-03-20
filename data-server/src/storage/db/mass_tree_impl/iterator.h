_Pragma("once");

#include "storage/db/iterator_interface.h"
#include "mass_tree_impl.h"
#include "scaner.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class MassTreeIterator: public IteratorInterface {
public:
    MassTreeIterator() = delete;
    ~MassTreeIterator() = default;

    MassTreeIterator(TreeType* tree, std::unique_ptr<threadinfo, ThreadInfoDeleter>& thread_info,
                     const std::string start, const std::string limit):
            start_(start), limit_(limit),
            scaner_(tree, thread_info, start, limit) {

    }

    bool Valid();
    void Next();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    const std::string start_;
    const std::string limit_;
    std::string it_key_ = "";
    std::string it_value_ = "";
    std::string last_key_ = "";

    Scaner scaner_;
};

}
}
}
