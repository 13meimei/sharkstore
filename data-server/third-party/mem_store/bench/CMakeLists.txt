set (bench_src
        bench.cpp
        ../src/skiplist.cc
        )

include_directories(..)
add_executable(bench ${bench_src})
target_link_libraries(bench profiler pthread)

