_Pragma("once");

#include <string>
#include <vector>

#include "mass_types.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class MassTreeDB;

class Scaner {
public:
    Scaner(MassTreeDB* tree, const std::string& vbegin, const std::string& vend, size_t max_rows);

    template<typename SS, typename K>
    void visit_leaf(const SS &, const K &, threadinfo &) {}

    bool visit_value(Masstree::Str key, std::string* value, threadinfo &);

    bool Valid();
    void Next();
    std::string Key() { return it_kv_.first; }
    std::string Value() { return it_kv_.second; }

private:
    bool scan_valid();
    bool iter_valid();
    void do_scan();
    void reset();

private:
    using KVPair = std::pair<std::string, std::string>;
    using KVPairVector = std::vector<std::pair<std::string, std::string>>;

    MassTreeDB* tree_ = nullptr;

    const std::string vbegin_;
    const std::string vend_;
    const size_t max_rows_ = 100;
    size_t rows_ = 0;

    bool last_flag_ = false;
    std::string last_key_;
    KVPairVector kvs_;
    KVPairVector::iterator kvs_it_;
    KVPair it_kv_;
};

}
}
}
