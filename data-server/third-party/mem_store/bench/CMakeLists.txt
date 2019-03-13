set (bench_src
        bench.cpp
        ../src/skiplist.cc
        )

include_directories(..)
add_executable(bench ${bench_src})
target_link_libraries(bench profiler pthread)

add_executable(tbb_hash tbb_hash.cpp)
target_link_libraries(tbb_hash tbb pthread profiler)

#add_executable(tree tree_bench.cpp)
#target_link_libraries(tree tbb pthread profiler)

