#include <string>

#include <proto/gen/watchpb_v1.pb.h>
#include <common/socket_session.h>
#include <mutex>

namespace sharkstore {
    namespace dataserver {
        namespace range {

            enum WATCH_CODE {
                WATCH_OK = 0,
                WATCH_NOT_EXIST,
            };

            class WatcherSet {
            public:
                WatcherSet() {
                };
                ~WatcherSet() {};
                WATCH_CODE AddWatcher(std::string, common::SocketSession *);
                void DelWatcher(std::string);
                std::vector<common::SocketSession*> GetWatchers(std::string);

            private:
                std::map<std::string, std::map<int64_t, common::SocketSession *>> watcher_set_;
                std::mutex mutex_;
            };


            WATCH_CODE WatcherSet::AddWatcher(std::string name, common::SocketSession* session) {
                std::lock_guard(mutex_);

                return WATCH_OK;
            }

            void WatcherSet::DelWatcher(std::string name) {
                std::lock_guard(mutex_);

            }

            std::vector<common::SocketSession*> WatcherSet::GetWathers(std::string name) {
                std::vector vec;
                std::lock_guard(mutex_);

                return vec;
            }
        }
    }
}