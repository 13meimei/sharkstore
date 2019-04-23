_Pragma("once")

#include <memory>
#include "raft/raft_log_reader.h" 
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"


using namespace sharkstore;
using namespace sharkstore::raft;


namespace sharkstore {
namespace test {
namespace mock {


class RaftLogReaderMock : public RaftLogReader {
    public:
        RaftLogReaderMock() = default;
        ~RaftLogReaderMock() = default; 

        RaftLogReaderMock(const RaftLogReaderMock &) = delete;
        RaftLogReaderMock& operator = (const RaftLogReaderMock &) = delete;

        Status GetData(const uint64_t idx, std::shared_ptr<raft_cmdpb::Command> & cmd) override;
        Status Close() override;

};

std::shared_ptr<RaftLogReader> CreateRaftLogReader();

} /* namespace mock */
} /* namespace test */
} /* namespace sharkstore */
