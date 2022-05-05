#pragma once

#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "rdma_utils.h"

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)
#define CHECK_WORKING

namespace rdma {

constexpr char *DEFAULT_PORT = (char *)"51216";
constexpr int DEFAULT_MAX_WR = 64;
constexpr int DEFAULT_MAX_QP = 64;
constexpr int DEFAULT_MAX_CLIENT_QP = 1;
constexpr int DEFAULT_MSG_LENGTH = 1024;
constexpr int DEFAULT_BUF_LENGTH = 8192;

enum rdma_id_state {
  RDMA_ID_STATE_FREE = 0,
  RDMA_ID_STATE_CONNECTED = 1,
  RDMA_ID_STATE_DISCONNECTED = 2,
  RDMA_ID_STATE_TIMEWAIT_EXITED = 3,
  RDMA_ID_STATE_QP_DESTROYED = 4,
  RDMA_ID_STATE_ID_DESTROYED = 5,
  RDMA_ID_STATE_READING = 6,
  RDMA_ID_STATE_WRITING = 7,
};

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
  std::atomic<enum rdma_id_state> *id_states;
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

enum async_request_type {
  ASYNC_READ = 0,
  ASYNC_WRITE = 1,
};

/* async ops request*/
struct AsyncRequest {
  // TODO: don't let any other thread modify the id_state if it is held by
  // AsyncRequest (just checking whether it is reading/writing is ok)
#ifdef CHECK_WORKING
  std::atomic<enum rdma_id_state> *id_state;
#endif
  enum async_request_type type;
  rdma_cm_id *cm_id;

  char *lm_addr;
  size_t lm_length;
  uint64_t rm_addr;

  int wait();
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
              struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey,
              AsyncRequest *async_request = nullptr);
  int write_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
               struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey,
               AsyncRequest *async_request = nullptr);
  int send(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *send_mr);
  int recv(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *recv_mr);

private:
  struct rdma::Context *ctx_;
  struct rdma_addrinfo *rai_;

  std::mutex mutex_;
  std::unordered_map<uint32_t, size_t> qp_num_2_conn_idx;

  int init_resources(struct rdma_addrinfo *rai);
  void destroy_resources();
  int await_completion();
  int wait_send_comp();

  int run_server();
  int connect_server(struct rdma_addrinfo *rai);
  int handle_connect_request(struct rdma_cm_event *cm_event);
  int handle_disconnected(struct rdma_cm_event *cm_event);
  int handle_timewait_exit(struct rdma_cm_event *cm_event);
};

} // namespace rdma