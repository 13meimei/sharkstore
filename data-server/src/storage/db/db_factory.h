_Pragma("once");

#include <memory>
#include <vector>

#include "base/status.h"
#include "common/ds_config.h"
#include "db_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status OpenDB(const ds_config_t& config, DbInterface** db);

} // namespace storage
} // namespace dataserver
} // namespace sharkstore
