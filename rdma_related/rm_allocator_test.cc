#include "cache/remote_memory/block_based_rm_allocator.h"
#include "cache/remote_memory/ff_based_rm_allocator.h"
#include <iostream>


int main() {
    rocksdb::FFBasedRemoteMemoryAllocator allocator;
    allocator.init(0x7fc776440010, 0x20000);
    
    allocator.rmalloc(0xf9d);
    allocator.rmalloc(0xf9e);
    allocator.rmalloc(0xf9c);
    allocator.rmalloc(0xf9e);


}