_Pragma("once");

namespace sharkstore {
namespace net {

class Statistics {
public:
    std::atomic<int64_t> session_count = {0};
    std::atomic<int64_t> recv_msg_count = {0};
    std::atomic<int64_t> recv_bytes_count = {0};
    std::atomic<int64_t> sent_msg_cout = {0};
    std::atomic<int64_t> sent_bytes_count = {0};

    void AddSessionCount(int64_t deta) {
        session_count += deta;
    }

    void AddMessageRecv(int64_t size) {
        ++recv_msg_count;
        recv_bytes_count += size;
    }

    void AddMessageSent(int64_t size) {
        ++sent_msg_cout;
        sent_bytes_count += size;
    }
};


}  // namespace net
}  // namespace sharkstore
