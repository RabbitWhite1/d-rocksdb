# CMAKEFILE REF: 
#   https://blog.csdn.net/FPGATOM/article/details/104696543
#   https://www.cnblogs.com/chaofn/p/10160555.html
cmake_minimum_required(VERSION 3.12)

project(rmalloc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_C_COMPILER /usr/bin/gcc-7)
set(CMAKE_CXX_COMPILER /usr/bin/g++-7)
set(CMAKE_CXX_FLAGS "-Werror")

find_library(LIB_IBVERBS NAMES ibverbs)
find_library(LIB_RDMACM NAMES rdmacm)

# specify rocksdb lib location here.
set(LIB_ROCKSDB_PATH $ENV{HOME}/.local/lib)
find_library(LIB_ROCKSDB NAMES rocksdb rocksdb_debug PATHS ${LIB_ROCKSDB_PATH} NO_DEFAULT_PATH)
if(NOT(${LIB_ROCKSDB} STREQUAL LIB_ROCKSDB-NOTFOUND))
    message("Using RocksDB lib: " ${LIB_ROCKSDB})
else(not ${LIB_ROCKSDB})
    message(FATAL_ERROR "RocksDB lib not found in: " ${LIB_ROCKSDB_PATH})
endif()

include_directories(.)

# aux_source_directory(. SOURCES)
set(SOURCES
  cache/remote_memory/rm.cc
  cache/remote_memory/ff_based_rm_allocator.cc
  cache/remote_memory/block_based_memory_allocator.cc
  cache/remote_memory/block_based_rm_allocator.cc
  cache/remote_memory/lm.cc
  cache/remote_memory/rdma_transport.cc
  cache/remote_memory/rdma_utils.cc
)

set(LIBS
  ${LIB_ROCKSDB}
  ${LIB_IBVERBS}
  ${LIB_RDMACM}
  tbb
  snappy
  z
  lz4
  bz2
  zstd
  dl
  pthread
)

add_executable(rdma_transport_test rdma_transport_test.cc ${SOURCES})
target_link_libraries(rdma_transport_test ${LIBS})

add_executable(rm_server rm_server.cc ${SOURCES})
target_link_libraries(rm_server ${LIBS})

add_executable(rm_test rm_test.cc ${SOURCES})
target_link_libraries(rm_test ${LIBS})

add_executable(lm_test lm_test.cc ${SOURCES})
target_link_libraries(lm_test ${LIBS})

add_executable(rm_allocator_test rm_allocator_test.cc ${SOURCES})
target_link_libraries(rm_allocator_test ${LIBS})
