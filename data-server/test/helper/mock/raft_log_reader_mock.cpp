#include "raft_log_reader_mock.h"

namespace sharkstore {
namespace test {
namespace mock {

Status RaftLogReaderMock::GetData( const uint64_t idx, std::shared_ptr<raft_cmdpb::Command> &cmd)
{
    cmd = std::make_shared<raft_cmdpb::Command>();
    cmd->mutable_cmd_id()->set_node_id(1);
    cmd->mutable_cmd_id()->set_seq(1);

    cmd->set_cmd_type(raft_cmdpb::CmdType::RawPut);
    cmd->mutable_verify_epoch()->set_conf_ver(1);
    cmd->mutable_verify_epoch()->set_version(1);

    cmd->mutable_kv_raw_put_req()->set_key("010030010");
    cmd->mutable_kv_raw_put_req()->set_value("01003001:value"); 
    

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
