add_library(fbase-raft-testutil STATIC test_util.cpp)

set(raft_unit_DEPS
    fbase-raft-testutil
    fbase-raft
    fbase-common
    fbase-frame
    fbase-base
    fbase-lkqueue
    fbase-proto
    fastcommon
    gtest
    ${PROTOBUF_LIBRARY}
    pthread
)

set (raft_unit_TESTS
    disk_storage_unittest.cpp
    log_file_unittest.cpp
    meta_file_unittest.cpp
    replica_unittest.cpp
    raft_log_unittest.cpp
    raft_types_unittest.cpp
    log_unstable_unittest.cpp
    snapshot_send_unittest.cpp
)

ENABLE_TESTING()
foreach(testfile ${raft_unit_TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${raft_unit_DEPS})
    add_test(NAME ${exename} COMMAND ${exename})
endforeach(testfile ${TESTS})

