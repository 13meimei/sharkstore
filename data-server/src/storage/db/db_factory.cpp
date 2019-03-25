#include "db_factory.h"

#include "rocksdb_impl/rocksdb_impl.h"
#include "skiplist_impl/skiplist_impl.h"
#include "mass_tree_impl/mass_tree_mvcc.h"
#ifdef SHARK_USE_BWTREE
#include "bwtree_impl/bwtree_db_impl.h"
#endif

namespace sharkstore {
namespace dataserver {
namespace storage {

Status OpenDB(const ds_config_t& config, DbInterface** db) {
    std::string engine_name(config.engine_config.name);
    if (strcasecmp(engine_name.c_str(), "rocksdb") == 0) {
        print_rocksdb_config();
        *db = new RocksDBImpl(config.rocksdb_config);
    } else if (strcasecmp(engine_name.c_str(), "memory") == 0) {
        *db = new SkipListDBImpl();
    } else if (strcasecmp(engine_name.c_str(), "mass-tree") == 0) {
        *db = new MvccMassTree();
    } else if (strcasecmp(engine_name.c_str(), "bwtree") == 0) {
#ifdef SHARK_USE_BWTREE
        *db = new storage::BwTreeDBImpl();
#else
        return Status(Status::kNotSupported, "bwtree", "confirm build opition ENABLE_BWTREE is on");
#endif
    } else {
        return Status(Status::kNotSupported, "unknown engine name", engine_name);
    }

    assert(db != nullptr);
    return (*db)->Open();
}



} // namespace storage
} // namespace dataserver
} // namespace sharkstore
