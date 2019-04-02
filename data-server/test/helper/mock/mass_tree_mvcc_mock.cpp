#include "mass_tree_mvcc_mock.h"
#include "mass_tree_iterator_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

IteratorInterface* MvccMassTreeMock::newIterMock(MassTreeDB *tree, const std::string &start, const std::string &limit) {
    auto version = mvcc_.insert();
    MultiVersionKey start_key(start, version, true);
    MultiVersionKey end_key(limit, std::numeric_limits<uint64_t>::max(), true);
    auto scaner = tree->NewScaner(start_key.to_string(), limit.empty() ? "" : end_key.to_string());
    return new MassTreeIteratorMock(std::move(scaner), version, [this, version] { mvcc_.erase(version); }, seek_);
}

}
}
}