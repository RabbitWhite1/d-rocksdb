#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>


#define N 4863

int main(int argc, char **argv) {
  std::string server_name("10.0.0.5");

  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    rocksdb::RemoteMemory rm(server_name, 536870912);
    uint64_t addr = rm.rmalloc(N);
    char buf_write[N];
    memset(buf_write, 'a', N);
    rm.write(addr, buf_write, N);
    char buf_read[N];
    rm.read(addr, buf_read, N);
    printf("%s\n", rdma::char_array_to_string(buf_read, N).c_str());
    rm.rmfree(addr);
  }
}