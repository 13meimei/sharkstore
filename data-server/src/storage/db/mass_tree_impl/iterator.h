_Pragma("once");

#include <memory>
#include <functional>
#include "storage/db/iterator_interface.h"
#include "storage/db/multi_v_key.h"
#include "base/status.h"

namespace sharkstore { namespace test { namespace mock { class MassTreeIteratorMock; }}}

namespace sharkstore {
namespace dataserver {
namespace storage {

class Scaner;
class MvccMassTree;

class MassTreeIterator: public IteratorInterface {

public:
    friend class ::sharkstore::test::mock::MassTreeIteratorMock;
    using Releaser = std::function<void()>;

    MassTreeIterator(std::unique_ptr<Scaner> scaner, uint64_t version,
            const Releaser& release_func, bool seek = true);
    ~MassTreeIterator();

    bool Valid();
    void Next();
    Status status();
    std::string key();
    std::string value();
    uint64_t key_size();
    uint64_t value_size();

private:
    void seek();

private:
    std::unique_ptr<Scaner> scaner_;
    uint64_t ver_;
    Releaser releaser_;
    MultiVersionKey cur_key_;
    bool cur_assigned_ = false;
};

}
}
}
