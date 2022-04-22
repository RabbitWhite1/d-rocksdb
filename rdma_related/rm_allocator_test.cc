#include "cache/remote_memory/rm_allocator.h"
#include <iostream>


int main() {
    rocksdb::RemoteMemoryAllocator allocator(0x7fc776440010, 0x20000);
    allocator.rmalloc(0xf9d);
    allocator.rmalloc(0xf9e);
    allocator.rmalloc(0xf9c);
    allocator.rmalloc(0xf9e);
    allocator.rmfree(0x7fc776442ee7);
    allocator.rmalloc(0xf9e);
    allocator.rmalloc(0xf9c);
    allocator.rmfree(0x7fc776440fad);
    
    allocator.rmalloc(0xf9b);
    allocator.rmfree(0x7fc776442ee7);
    
    allocator.rmalloc(0xf9d);
    allocator.print();
    allocator.rmalloc(0xf9c);
    allocator.print();
    allocator.rmalloc(0xf9c);
    allocator.print();


}