# CMAKEFILE REF: 
#   https://blog.csdn.net/FPGATOM/article/details/104696543
#   https://www.cnblogs.com/chaofn/p/10160555.html
cmake_minimum_required(VERSION 3.12)

project(ycsbcpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_C_COMPILER /usr/bin/gcc-7)
set(CMAKE_CXX_COMPILER /usr/bin/g++-7)

# specify rocksdb lib location here.
set(LIB_ROCKSDB_PATH $ENV{HOME}/.local/lib)
find_library(LIB_ROCKSDB NAMES rocksdb rocksdb_debug PATHS ${LIB_ROCKSDB_PATH} NO_DEFAULT_PATH)
if(NOT(${LIB_ROCKSDB} STREQUAL LIB_ROCKSDB-NOTFOUND))
    message("Using RocksDB lib: " ${LIB_ROCKSDB})
else(not ${LIB_ROCKSDB})
    message(FATAL_ERROR "RocksDB lib not found in: " ${LIB_ROCKSDB_PATH})
endif()

set(CORE_DIR ./core)
set(ROCKSDB_DIR ./rocksdb)

include_directories(.)

aux_source_directory(./core SOURCES)
aux_source_directory(./rocksdb SOURCES)
add_executable(ycsb ${SOURCES})

target_link_libraries(ycsb ${LIB_ROCKSDB} tbb snappy z lz4 bz2 zstd dl pthread)
