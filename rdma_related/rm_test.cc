#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>


#define N 4863

int main(int argc, char **argv) {
  std::string server_name("10.0.0.5");

  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    rocksdb::RemoteMemory rm(server_name, 0x20000);
    rm.rmalloc(0xf9d);
  }
}