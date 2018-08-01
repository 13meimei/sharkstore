include_directories(${PROJECT_SOURCE_DIR}/test)

set(test_helper_SRCS
    helper/mock/raft_mock.cpp
    helper/mock/raft_server_mock.cpp
    helper/mock/socket_session_mock.cpp
    helper/gen/test.pb.cc
    helper/helper_util.cpp
    helper/query_builder.cpp
    helper/query_parser.cpp
    helper/store_test_fixture.cpp
    helper/table.cpp
)

add_library(shark-test-helper SHARED ${test_helper_SRCS})

set (test_DEPEND_LIBS
    shark-test-helper
    sharkstore-ds-shared
    ${GTEST_LIBRARY}
    ${depend_LIBRARYS}
)

set(test_SRCS
    fast_net_client.cpp
    fast_net_server.cpp
    unittest/encoding_unittest.cpp
    unittest/field_value_unittest.cpp
    unittest/meta_store_unittest.cpp
    unittest/monitor_unittest.cpp
    unittest/range_ddl_unittest.cpp
    unittest/range_dml_unittest.cpp
    unittest/range_raw_unittest.cpp
    unittest/row_decoder_unittest.cpp
    unittest/status_unittest.cpp
    unittest/store_unittest.cpp
    unittest/util_unittest.cpp
)

foreach(f IN LISTS test_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

foreach(testfile ${test_SRCS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    if("${exename}" MATCHES "unittest")
        add_test(NAME ${exename} COMMAND ${exename})
    endif()
endforeach(testfile ${TESTS})

if (BUILD_NET)
    add_subdirectory(net)
endif()
