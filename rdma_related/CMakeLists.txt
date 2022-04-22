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

include_directories(.)

# aux_source_directory(. SOURCES)
set(SOURCES
  cache/remote_memory/rm.cc
  cache/remote_memory/rm_allocator.cc
  cache/remote_memory/rdma_transport.cc
  cache/remote_memory/rdma_utils.cc
)

add_executable(rdma_transport_test rdma_transport_test.cc ${SOURCES})
target_link_libraries(rdma_transport_test ${LIB_IBVERBS} ${LIB_RDMACM})

add_executable(rm_test rm_test.cc ${SOURCES})
target_link_libraries(rm_test ${LIB_IBVERBS} ${LIB_RDMACM})

add_executable(rm_allocator_test rm_allocator_test.cc ${SOURCES})
target_link_libraries(rm_allocator_test ${LIB_IBVERBS} ${LIB_RDMACM})
