# generate config.h
if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/config.h")
    execute_process(
            COMMAND ./bootstrap.sh
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    execute_process(
            COMMAND env CXXFLAGS=-std=c++11; ./configure --disable-superpage --enable-max-key-len=1048576
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/config.h")

set(kvtreeSRCS
    json.cc
    straccum.cc
    string.cc
    clp.c
    compiler.cc
    memdebug.cc
    kvthread.cc
    config.h
)
add_library(mass-tree STATIC ${kvtreeSRCS})

add_executable(simple-bench simple_bench.cpp)
target_link_libraries(simple-bench mass-tree pthread)
