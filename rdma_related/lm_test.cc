#include "cache/remote_memory/block_based_rm_allocator.h"
// #include "cache/remote_memory/rdma_transport.h"
// #include "cache/remote_memory/rdma_utils.h"
#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rm_async_request.h"
#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {
  int ret;
  char server_name[] = "10.0.0.7";
  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    std::string buf = "Hello World!\0";
    rocksdb::RemoteMemory rm0(
        new rocksdb::BlockBasedRemoteMemoryAllocator(4096), server_name, 8192,
        0);
    rocksdb::RemoteMemory rm1(
        new rocksdb::BlockBasedRemoteMemoryAllocator(4096), server_name, 8192,
        0);
    auto lm0 = std::make_shared<rocksdb::LocalMemory>(
        new rocksdb::BlockBasedMemoryAllocator(16384, 4096));

    rm0.init_lm(lm0);
    rm1.init_lm(lm0);

    auto lm0_mr0 = (rocksdb::MemoryRegion *)lm0->get_allocator()->Allocate(4096);
    auto lm0_mr1 = (rocksdb::MemoryRegion *)lm0->get_allocator()->Allocate(4096);
    auto lm0_mr2 = (rocksdb::MemoryRegion *)lm0->get_allocator()->Allocate(4096);
    auto lm0_mr3 = (rocksdb::MemoryRegion *)lm0->get_allocator()->Allocate(4096);
    memcpy(lm0_mr0->addr, buf.c_str(), buf.size());
    memcpy(lm0_mr1->addr, buf.c_str(), buf.size());
    memcpy(lm0_mr2->addr, buf.c_str(), buf.size());
    memcpy(lm0_mr3->addr, buf.c_str(), buf.size());
    auto rm0_mr0 = rm0.rmalloc(4096);
    auto rm0_mr1 = rm0.rmalloc(4096);
    auto rm1_mr0 = rm1.rmalloc(4096);
    auto rm1_mr1 = rm1.rmalloc(4096);

    auto rm_async_direct_request0 = new rocksdb::RMAsyncDirectRequest();
    rm0.direct_write(rm0_mr0->addr, lm0_mr0->addr, buf.size(), rm_async_direct_request0);
    // auto rm_async_direct_request1 = new rocksdb::RMAsyncDirectRequest();
    // rm1.direct_write(rm1_mr0->addr, lm0_mr1->addr, buf.size(), rm_async_direct_request1);
    auto rm_async_direct_request2 = new rocksdb::RMAsyncDirectRequest();
    rm0.direct_read(rm0_mr1->addr, lm0_mr1->addr, buf.size(), rm_async_direct_request2);

    rm_async_direct_request0->wait();
    rm_async_direct_request0->unlock_and_reset();
    // rm_async_direct_request1->wait();
    // rm_async_direct_request1->unlock_and_reset();
    rm_async_direct_request2->wait();
    rm_async_direct_request2->unlock_and_reset();
  }
}