
set (test_MOCK_LIBS
    sharkstore-ds-shared
    test-mock-shared
    ${GTEST_LIBRARY}
    ${depend_LIBRARYS}
)

set (TESTS
    watch_unittest.cpp
    watch_unittest_get.cpp
    watch_unittest_put.cpp
    watch_unittest_del.cpp
    watch_unittest_watch.cpp
    watch_unittest_lock.cpp
    #watcher_unittest.cpp
)

foreach(f IN LISTS TESTS)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()


foreach(testfile ${TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    if("${exename}" MATCHES "unittest")
        add_test(NAME ${exename} COMMAND ${exename})
    endif()
endforeach(testfile ${TESTS})

