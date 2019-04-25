_Pragma("once");

#include "masstree-beta/config.h"
#include "masstree-beta/masstree.hh"
#include "masstree-beta/timestamp.hh"
#include "masstree-beta/kvthread.hh"

namespace sharkstore {
namespace dataserver {
namespace storage {

struct ThreadInfoDeleter {
    void operator()(threadinfo*) const {}
};

class StringValuePrinter {
public:
    static void print(std::string *value, FILE* f, const char* prefix,
                      int indent, Masstree::Str key, kvtimestamp_t,
                      char* suffix) {
        fprintf(f, "%s%*s%.*s = %s%s\n",
                prefix, indent, "", key.len, key.s, (value ? value->c_str() : ""), suffix);
    }
};

struct default_query_table_params : public Masstree::nodeparams<15, 15> {
    typedef std::string* value_type;
    typedef StringValuePrinter value_print_type;
    typedef ::threadinfo threadinfo_type;
};

using TreeType = Masstree::basic_table<default_query_table_params>;

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
