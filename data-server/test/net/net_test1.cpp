#include <asio/steady_timer.hpp>
#include <iostream>
#include "net/io_context_pool.h"

using namespace sharkstore::dataserver::net;

int main(int argc, char *argv[]) {
    IOContextPool pool(10);

    pool.Start();

    asio::steady_timer t(pool.GetIOContext(), asio::chrono::seconds(5));
    t.wait();

    pool.Stop();

    std::cout << "stoppped" << std::endl;

    return 0;
}
