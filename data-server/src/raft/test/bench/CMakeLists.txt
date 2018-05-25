set(raft_bench_SOURCES
    address.cpp
    config.cpp
    fast_service.cpp
    main.cpp
    node.cpp
    range.cpp
    )

add_executable(raft-bench ${raft_bench_SOURCES})
target_link_libraries(raft-bench ${raft_test_Deps} profiler)
