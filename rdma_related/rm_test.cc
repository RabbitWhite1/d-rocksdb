#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>

int main(int argc, char **argv) {
  std::string server_name("10.0.0.5");

  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    rocksdb::RemoteMemory rm(server_name, 32768);
    uint64_t addr = rm.rmalloc(1024);
    rm.write(addr, (void *)"Hello World!", strlen("Hello World!"));
    char buf[1024];
    rm.read(addr, buf, strlen("Hello World!"));
    printf("%s\n", rdma::char_array_to_string(buf, strlen("Hello World!")).c_str());
    rm.rmfree(addr);
  }
}