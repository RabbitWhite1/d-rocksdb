#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>
#include <unistd.h>


#define N 4863

int main(int argc, char **argv) {
  std::string server_name("10.0.0.5");

  if (argc > 1) {
    rocksdb::RemoteMemoryServer rm_server(server_name);
  } else {
    std::string str = "Hello World!";
    rocksdb::RemoteMemory rm(server_name, 0x20000);
    uint64_t rm_addr = rm.rmalloc(str.size());
    rm.write(rm_addr, (void *)str.c_str(), str.size());
    printf("wrote!\n");
    sleep(5);
  }
}