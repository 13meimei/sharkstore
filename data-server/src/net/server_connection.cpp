#include "server_connection.h"

namespace fbase {
namespace dataserver {
namespace net {

std::atomic<uint64_t> ServerConnection::total_count_ = {0};

ServerConnection::ServerConnection(const ConnectionOptions& opt,
                                   asio::io_context& context)
    : opt_(opt), io_context_(context), socket_(context) {
    ++total_count_;
}

ServerConnection::~ServerConnection() { --total_count_; }

} /* net */
} /* dataserver  */
} /* fbase  */
