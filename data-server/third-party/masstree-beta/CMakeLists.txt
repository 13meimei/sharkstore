add_custom_command(OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/config.h
        COMMAND echo "GENERATING MassTree config.h"
        COMMAND ./bootstrap.sh
        COMMAND ./configure --disable-superpage
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )

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
