#include <common/socket_session.h>
#include "watch.hpp"
#include "range.h"

namespace sharkstore {
    namespace dataserver {
        namespace range {
            WATCH_CODE Range::AddKeyWatcher(std::string name, common::ProtoMessage* msg) {
                return key_watchers_.AddWatcher(name, msg);
            }

            void Range::DelKeyWatcher(std::string name) {
                return key_watchers_.DelWatcher(name);
            }

            std::vector<common::ProtoMessage*> Range::GetKeyWatchers(std::string name) {
                return key_watchers_.GetWatchers(name);
            }
        }
    }
}