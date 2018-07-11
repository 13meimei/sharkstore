//#include "watch.h"
#include <common/socket_session.h>
#include "watch.hpp"
#include "range.h"

namespace sharkstore {
    namespace dataserver {
        namespace range {
            void Range::AddKeyWatcher(std::string name, common::ProtoMessage* msg) {
                return key_watchers_.AddWatcher(name, msg);
            }

            WATCH_CODE Range::DelKeyWatcher(int64_t id) {
                return key_watchers_.DelWatcher(id);
            }

            uint32_t Range::GetKeyWatchers(std::vector<common::ProtoMessage*>& vec, std::string &name) {
                return key_watchers_.GetWatchers(vec, name);
            }
        }
    }
}
