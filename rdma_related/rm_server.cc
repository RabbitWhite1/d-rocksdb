#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>

int main(int argc, char **argv) {
  assert (argc > 1);
  std::string server_name(argv[1]);
  rocksdb::RemoteMemoryServer rm_server(server_name);
}