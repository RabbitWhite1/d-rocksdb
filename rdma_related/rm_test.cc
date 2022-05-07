#include "cache/remote_memory/block_based_rm_allocator.h"
// #include "cache/remote_memory/rdma_transport.h"
// #include "cache/remote_memory/rdma_utils.h"
#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rm_async_request.h"
#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {
  int ret;
  char server_name[] = "10.0.0.5";
  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    std::string buf = "Hello World!\0";
    rocksdb::RemoteMemory rm0(
        new rocksdb::BlockBasedRemoteMemoryAllocator(4096), server_name, 8192,
        0);
    // rocksdb::RemoteMemory rm1(
    //     new rocksdb::BlockBasedRemoteMemoryAllocator(4096), server_name, 8192,
    //     1);

    rocksdb::RMAsyncRequest *rm_async_request1 = new rocksdb::RMAsyncRequest();
    rocksdb::RMAsyncRequest *rm_async_request2 = new rocksdb::RMAsyncRequest();
    rocksdb::RMRegion *shard0_region1 = rm0.rmalloc(4096);
    rocksdb::RMRegion *shard0_region2 = rm0.rmalloc(4096);

    rm0.read(shard0_region1->addr, nullptr, buf.size(), rm_async_request1);
    rm0.write(shard0_region2->addr, nullptr, buf.size(), rm_async_request2);

    rm_async_request1->wait();
    rm_async_request1->unlock_and_reset();
    sleep(1);
    rm_async_request2->wait();
    rm_async_request2->unlock_and_reset();

    // rm_async_request->wait();
    // rm_async_request->unlock_and_reset();
    // rm_async_request->rdma_async_request->wait();

    // rm0.write(shard0_region0->addr, (void *)buf.c_str(), buf.size(), rm_async_request);
    // rm_async_request->wait();
    // rm_async_request->unlock_and_reset();
  }
}