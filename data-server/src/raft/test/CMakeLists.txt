add_executable(single_node single_node.cpp)
target_link_libraries(single_node ${raft_test_Deps})

add_executable(three_node three_node.cpp)
target_link_libraries(three_node ${raft_test_Deps})

add_subdirectory(bench)
add_subdirectory(unittest)
if (RAFT_BUILD_PLAYGROUND) 
    add_subdirectory(playground)
endif()
