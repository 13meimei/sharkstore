set (bench_src
        bench.cpp
        ../src/skiplist.cc
        )

include_directories(..)
add_executable(bench ${bench_src})
target_link_libraries(bench profiler pthread)

#include_directories(../../../third-party/masstree-beta)
#set (masstree_bench_src
#        ./mass_tree.cpp
#        ../../../src/storage/db/mass_tree_impl/mass_tree_impl.cpp
#        ../../../src/base/status.cpp
#        ../../../src/storage/db/memdb_batch.cpp
#        ../../../src/sharkstore/data-server/third-party/masstree-beta/kvthread.cc
#        )
#
#add_executable(masstree_bench ${masstree_bench_src})
#target_link_libraries(masstree_bench profiler pthread)

