#include "raft_log_reader_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

Status RaftLogReaderMock::GetData( const uint64_t idx, std::shared_ptr<raft_cmdpb::Command> &cmd)
{
    
    
    

    return Status::OK();
}

Status RaftLogReaderMock::Close()
{

    return Status::OK();
} 

std::shared_ptr<RaftLogReader> CreateRaftLogReader()
{
    return std::shared_ptr<RaftLogReader>(new RaftLogReaderMock());
}


} /* namespace mock */
} /* namespace test */
} /* namespace sharkstore */
