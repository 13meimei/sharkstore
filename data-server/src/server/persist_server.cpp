#include "persist_server.h"
#include "persist_server_impl.h"

namespace sharkstore {
namespace dataserver {
namespace server {

std::unique_ptr<PersistServer> CreatePersistServer(const PersistOptions & ops)
{
    return std::unique_ptr<PersistServer>(new PersistServerImpl(ops));
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
