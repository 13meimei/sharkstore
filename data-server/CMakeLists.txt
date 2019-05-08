cmake_minimum_required(VERSION 2.6)
project(sharkstore-ds C CXX)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_STANDARD 11)

set(CXX_FLAGS
    -std=c++11
    -Wall
    -Wextra
    -Wformat
    -Wsign-compare
    -Wno-unused-parameter
    -Wno-unused-variable
    -Woverloaded-virtual
    -Wnon-virtual-dtor
    -Wno-missing-field-initializers
    -D__STDC_FORMAT_MACROS
    -DASIO_STANDALONE
    -mcx16  # bwtree flag
    -Wno-invalid-offsetof # bwtree flag
)

string(REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

set(CMAKE_C_FLAGS "-std=c99 -Wall -Wformat -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE")
if (APPLE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DDARWIN  -DIOEVENT_USE_KQUEUE")
elseif(UNIX)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOS_LINUX -DIOEVENT_USE_EPOLL")
else()
    message(FATAL_ERROR "Unsupported OS Type: " ${CMAKE_SYSTEM_NAME})
endif()

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2 -DNDEBUG -DBWTREE_NODEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O2 -DNDEBUG -DBWTREE_NODEBUG" )
    set(CMAKE_C_FLAGS_RELEASE "")
    set(CMAKE_CXX_FLAGS_RELEASE "")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -ggdb -DDEBUG_FLAG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g -ggdb -DDEBUG_FLAG")
endif()
MESSAGE(STATUS CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/raft/include)

# third-party
include_directories(${PROJECT_SOURCE_DIR}/third-party)
include_directories(${PROJECT_SOURCE_DIR}/third-party/asio-1-12-0/include)
include_directories(${PROJECT_SOURCE_DIR}/third-party/rapidjson-1.1.0/include)
add_subdirectory(${PROJECT_SOURCE_DIR}/third-party/mem_store)
include_directories(${PROJECT_SOURCE_DIR}/third-party/mem_store/include)
add_subdirectory(${PROJECT_SOURCE_DIR}/third-party/masstree-beta)

OPTION (ENABLE_BWTREE "Use BwTree" OFF)
MESSAGE(STATUS ENABLE_BWTREE=${ENABLE_BWTREE})
if (ENABLE_BWTREE)
    add_subdirectory(${PROJECT_SOURCE_DIR}/third-party/BwTree-baseline)
    include_directories(${PROJECT_SOURCE_DIR}/third-party/BwTree-baseline/src)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSHARK_USE_BWTREE")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSHARK_USE_BWTREE" )
endif()

# use intel tbb
OPTION (ENABLE_TBB "Use Intel TBB" ON)
MESSAGE(STATUS ENABLE_TBB=${ENABLE_TBB})
if(ENABLE_TBB)
    find_library(TBB_LIB tbb)
    if(NOT TBB_LIB)
        message(FATAL_ERROR "TBB library not found")
    else()
        message(STATUS "Found TBB C++ library: " ${TBB_LIB})
    endif()

    # find grpc header
    find_path(TBB_HEADER_PATH tbb/concurrent_queue.h)
    if(NOT TBB_HEADER_PATH )
        message(FATAL_ERROR "TBB headers not found")
    else()
        message(STATUS "Found TBB headers path: " ${TBB_HEADER_PATH})
        include_directories(${TBB_HEADER_PATH})
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTBB")
endif()

OPTION (ENABLE_TCMALLOC "Use TCMalloc" OFF)
if (ENABLE_TCMALLOC)
    MESSAGE(STATUS "Use TCMalloc")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSHARK_USE_TCMALLOC -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSHARK_USE_TCMALLOC -fno-omit-frame-pointer")
    find_library(TCMALLOC_LIB tcmalloc)
    if(NOT TCMALLOC_LIB )
        message(FATAL_ERROR "TCMalloc library not found")
    else()
        message(STATUS "Found TCMalloc C++ library: " ${TCMALLOC_LIB})
    endif()
endif()

# find protobuf
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})


# find grpc
find_library(GRPC_LIB grpc)
if(NOT GRPC_LIB)
    message(FATAL_ERROR "Grpc library not found")
else()
    message(STATUS "Found Grpc library: " ${GRPC_LIB})
endif()

find_library(GRPC_CPP_LIB grpc++)
if(NOT GRPC_CPP_LIB)
    message(FATAL_ERROR "Grpc++ library not found")
else()
    message(STATUS "Found Grpc C++ library: " ${GRPC_CPP_LIB})
endif()

# find grpc header
find_path(GRPC_HEADER_PATH grpc/grpc.h)
if(NOT GRPC_HEADER_PATH )
    message(FATAL_ERROR "Grpc headers not found")
else()
    message(STATUS "Found Grpc headers path: " ${GRPC_HEADER_PATH})
    include_directories(${GRPC_HEADER_PATH})
endif()

# find rocksdb
find_library(ROCKSDB_LIB librocksdb.a rocksdb)
if(NOT ROCKSDB_LIB)
    message(FATAL_ERROR "Rocksdb library not found")
else()
    message(STATUS "Found Rocksdb library: " ${ROCKSDB_LIB})
endif()

find_path(ROCKSDB_HEADER_PATH rocksdb/db.h)
if(NOT ROCKSDB_HEADER_PATH)
    message(FATAL_ERROR "Rocksdb headers not found")
else()
    message(STATUS "Found Rocksdb headers path: " ${ROCKSDB_HEADER_PATH})
    include_directories(${ROCKSDB_HEADER_PATH})
endif()

# find fastcommon
find_library(FASTCOMMON_LIB fastcommon)
if(NOT FASTCOMMON_LIB)
    message(FATAL_ERROR "Fastcommon library not found")
else()
    message(STATUS "Found Fastcommon library: " ${FASTCOMMON_LIB})
endif()

find_path(FASTCOMMON_HEADER_PATH fastcommon/fast_task_queue.h)
if(NOT FASTCOMMON_HEADER_PATH )
    message(FATAL_ERROR "Fastcommon headers not found")
else()
    message(STATUS "Found Fastcommon headers path: " ${FASTCOMMON_HEADER_PATH})
    include_directories(${FASTCOMMON_HEADER_PATH})
endif()

OPTION(BLOB_EXTEND_OPTIONS "Enable blob extend options" OFF)
MESSAGE(STATUS BLOB_EXTEND_OPTIONS=${BLOB_EXTEND_OPTIONS})
if(BLOB_EXTEND_OPTIONS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBLOB_EXTEND_OPTIONS")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBLOB_EXTEND_OPTIONS")
endif()

# use gcov
OPTION (ENABLE_COVERAGE "Use gcov" OFF)
MESSAGE(STATUS ENABLE_COVERAGE=${ENABLE_COVERAGE})
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

# profiler
OPTION (ENABLE_GPERF "Use gperf" OFF)
MESSAGE(STATUS ENABLE_GPERF=${ENABLE_GPERF})
if(ENABLE_GPERF)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_GPERF")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DUSE_GPERF" )
endif()

# gcc address sanitize
OPTION (ENABLE_SANITIZE "Use gcc address sanitize" OFF)
MESSAGE(STATUS ENABLE_SANITIZE=${ENABLE_SANITIZE})
if(ENABLE_SANITIZE )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address" )
endif()


message(STATUS "CMAKE_C_FLAGS: " ${CMAKE_C_FLAGS})
message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})

set(SOURCES
    src/server/main.cpp
    src/server/server.cpp
    src/server/worker.cpp
    src/server/node_address.cpp
    src/server/raft_logger.cpp
    src/server/run_status.cpp
    src/server/range_context_impl.cpp
    src/server/range_server.cpp
    src/server/rpc_server.cpp
    src/server/version.cpp
    src/range/range.cpp
    src/range/lock.cpp
    src/range/meta_keeper.cpp
    src/range/raw_get.cpp
    src/range/raw_put.cpp
    src/range/raw_delete.cpp
    src/range/select.cpp
    src/range/insert.cpp
    src/range/update.cpp
    src/range/delete.cpp
    src/range/split.cpp
    src/range/split_policy.cpp
    src/range/peer.cpp
	src/range/watch.cpp
	src/range/watch_funcs.cpp
    src/range/submit.cpp
    src/range/txn.cpp
    src/storage/aggregate_calc.cpp
    src/storage/field_value.cpp
    src/storage/kv_fetcher.cpp
    src/storage/meta_store.cpp
    src/storage/metric.cpp
    src/storage/row_decoder.cpp
    src/storage/row_fetcher.cpp
    src/storage/snapshot.cpp
    src/storage/store.cpp
    src/storage/db/memdb_batch.cpp
    src/storage/db/db_factory.cpp
    src/storage/db/rocksdb_impl/iterator.cpp
    src/storage/db/rocksdb_impl/write_batch.cpp
    src/storage/db/rocksdb_impl/rocksdb_impl.cpp
    src/storage/db/skiplist_impl/iterator.cpp
    src/storage/db/skiplist_impl/skiplist_impl.cpp
    src/storage/db/mass_tree_impl/mass_tree_db.cpp
    src/storage/db/mass_tree_impl/mass_tree_mvcc.cpp
    src/storage/db/mass_tree_impl/iterator.cpp
    src/storage/db/mass_tree_impl/scaner.cpp
    src/storage/store_watch.cpp
    src/storage/where_expr.cpp
    src/storage/store_txn.cpp
    src/storage/select_txn.cpp
    src/storage/util.cpp
    src/master/client.cpp
    src/master/connection.cpp
    src/master/rpc_types.cpp
    src/master/worker_impl.cpp
    src/monitor/histogram.cpp
    src/monitor/statistics.cpp
    src/monitor/system_status.cpp
	src/watch/watcher.cpp
	src/watch/watcher_set.cpp
	src/watch/watch_server.cpp
	src/watch/watch_event_buffer.cpp
    src/admin/admin_server.cpp
    src/admin/get_config.cpp
    src/admin/get_info.cpp
    src/admin/set_config.cpp
)

if (ENABLE_BWTREE)
    list(APPEND SOURCES src/storage/db/bwtree_impl/bwtree_db_impl.cpp)
endif()

if (APPLE)
    list(APPEND SOURCES src/monitor/mac_status.cpp)
elseif(UNIX)
    list(APPEND SOURCES src/monitor/linux_status.cpp)
else()
    message(FATAL_ERROR "Unsupported OS Type: " ${CMAKE_SYSTEM_NAME})
endif()

foreach(f IN LISTS SOURCES)
    # remove "src/"
    string(SUBSTRING ${f} 4 -1 fname)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"${fname}\"")
endforeach()

add_subdirectory(src/base)
add_subdirectory(src/common)
add_subdirectory(src/frame)
add_subdirectory(src/proto)
add_subdirectory(src/raft)
add_subdirectory(src/net)

set (depend_LIBRARYS
    sharkstore-proto
    sharkstore-raft
    sharkstore-net
    sharkstore-common
    sharkstore-frame
    sharkstore-base
    ${PROTOBUF_LIBRARY}
    ${GRPC_LIB}
    ${GRPC_CPP_LIB}
    ${ROCKSDB_LIB}
    ${FASTCOMMON_LIB}
    skiplist
    mass-tree
    pthread
    dl
    z
)
if(ENABLE_GPERF)
    list(APPEND depend_LIBRARYS profiler)
endif()

if (ENABLE_BWTREE)
    list(APPEND depend_LIBRARYS bwtree atomic)
endif()

if(ENABLE_TBB)
    list(APPEND depend_LIBRARYS ${TBB_LIB})
endif()

if (ENABLE_TCMALLOC)
    list(APPEND depend_LIBRARYS ${TCMALLOC_LIB})
endif()

message(STATUS "Depend Libraries: " "${depend_LIBRARYS}")

add_executable(data-server ${SOURCES})
target_link_libraries(data-server ${depend_LIBRARYS})

OPTION (BUILD_VERSION "build sharkstore version" ON)
MESSAGE(STATUS BUILD_VERSION=${BUILD_VERSION})
if (BUILD_VERSION)
    add_custom_target(
        gen-version ALL
        ${CMAKE_COMMAND} -DPROJECT_SOURCE_DIR=${PROJECT_SOURCE_DIR} -DBUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DBUILD_FLAGS=${CMAKE_CXX_FLAGS} -P ${PROJECT_SOURCE_DIR}/cmake/version.cmake
    )
    add_dependencies(data-server gen-version)
endif()

install(TARGETS data-server RUNTIME DESTINATION bin)


OPTION (BUILD_TEST "build sharkstore tests" OFF)
MESSAGE(STATUS BUILD_TEST=${BUILD_TEST})
if (BUILD_TEST)
    enable_testing()
    find_package(GTest REQUIRED)
    include_directories(${GTEST_INCLUDE_DIRS})
    add_library(sharkstore-ds-shared SHARED ${SOURCES})
    add_subdirectory(test)
endif()


OPTION(BUILD_TOOL "build sharstore tools" OFF)
MESSAGE(STATUS BUILD_TOOL=${BUILD_TOOL})
if (BUILD_TOOL)
    add_subdirectory(tools)
endif()
