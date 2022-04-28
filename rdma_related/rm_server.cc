#include "cache/remote_memory/rm.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>

int main(int argc, char **argv) {
  std::string server_name("10.0.0.5");
  rocksdb::RemoteMemoryServer rm_server(server_name);
}