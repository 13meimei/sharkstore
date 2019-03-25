_Pragma("once");

#include <memory>
#include <functional>
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
    using Releaser = std::function<void()>;

    MassTreeIterator(std::unique_ptr<Scaner> scaner, uint64_t version, const Releaser& release_func);
    ~MassTreeIterator();

    bool Valid();
    void Next();
    Status status();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    std::unique_ptr<Scaner> scaner_;
    uint64_t ver_;
    Releaser releaser_;
    MultiVersionKey cur_key_;
};

}
}
}
