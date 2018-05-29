set(net_TESTS
    net_test1.cpp
)

foreach(testfile ${net_TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
     target_link_libraries(${exename} sharkstore-net sharkstore-base pthread)
endforeach(testfile ${net_TESTS})
