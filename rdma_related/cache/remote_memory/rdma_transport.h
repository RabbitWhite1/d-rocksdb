#pragma once

#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <mutex>

#include "rdma_utils.h"

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

namespace rdma {

constexpr char *DEFAULT_PORT = (char *)"51216";
constexpr int DEFAULT_MAX_WR = 64;
constexpr int DEFAULT_MAX_QP = 1;
constexpr int DEFAULT_MSG_LENGTH = 1024;
constexpr int DEFAULT_BUF_LENGTH = 8192;

/* Resources used in the example */
struct Context {
  /* User parameters */
  bool server;
  char *server_name;
  char *server_port;
  int msg_count;
  int msg_length;
  int local_buf_size;
  int qp_count;
  int max_wr;
  /* Resources */
  struct rdma_cm_id *srq_id;
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_ids;
  struct rdma_event_channel *event_channel;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;
  struct ibv_mr *buf_mr;
  struct ibv_srq *srq;
  struct ibv_cq *srq_cq;
  struct ibv_comp_channel *srq_cq_channel;
  char *send_buf;
  char *recv_buf;
  /*
   * It is the remote memory for server, whose size is ; and
   * it is local buffer for client.
   */
  char *buf;
  /* remote memory info */
  uint32_t rm_rkey;
  uint64_t rm_addr;
  size_t rm_size;
};

class Transport {
  // TODO: target on 1 qp now, will extent it to qp pool.
public:
  // server
  Transport(bool server, const char *server_name);
  // client
  Transport(bool server, const char *server_name, size_t rm_size,
            size_t local_buf_size = DEFAULT_BUF_LENGTH);
  ~Transport();

  const struct Context *get_context() { return ctx_; }

  int read_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
              struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey);
  int write_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
               struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey);
  int send(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *send_mr);
  int recv(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *recv_mr);

private:
  struct rdma::Context *ctx_;
  struct rdma_addrinfo *rai_;

  int next_conn_ids_idx = 0;

  int init_resources(struct rdma_addrinfo *rai);
  void destroy_resources();
  int await_completion();
  int wait_send_comp();

  int run_server();
  int connect_server(struct rdma_addrinfo *rai);
  int handle_connect_request(struct rdma_cm_event *cm_event);
  int handle_disconnected(struct rdma_cm_event *cm_event);
};

} // namespace rdma