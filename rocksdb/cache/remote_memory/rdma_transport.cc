#include "rdma_transport.h"

#include <iostream>

int rdma::RDMAAsyncRequest::wait() {
  int ne;
  struct ibv_wc wc = {};

#ifdef CHECK_RDMA_CM_ID_WORKING
  if (type == ASYNC_READ) {
    if (not(id_state->load() == RDMA_ID_STATE_READING)) {
      printf("type=%d, id_state=%d\n", type, id_state->load());
      throw std::runtime_error("RDMAAsyncRequest::wait() failed");
    }
  } else {
    if (not(type == ASYNC_WRITE && id_state->load() == RDMA_ID_STATE_WRITING)) {
      printf("type=%d, id_state=%d\n", type, id_state->load());
      throw std::runtime_error("RDMAAsyncRequest::wait() failed");
    }
  }
#endif

  do {
    ne = ibv_poll_cq(cm_id->send_cq, 1, &wc);
    if (ne < 0) {
      printf("wc.status: %s\n", ibv_wc_status_str(wc.status));
      VERB_ERR("ibv_poll_cq", ne);
      throw std::runtime_error("ibv_poll_cq failed");
    }

    if (wc.status != IBV_WC_SUCCESS) {
      VERB_ERR("ibv_poll_cq", wc.status);
      printf("wc.status: %s\n", ibv_wc_status_str(wc.status));
      throw std::runtime_error("ibv_poll_cq failed");
    }
  } while (ne == 0);

  return 0;
}

int rdma::RDMAAsyncRequest::reset_cm_id_state() {
#ifdef CHECK_RDMA_CM_ID_WORKING
  *id_state = RDMA_ID_STATE_CONNECTED;
  printf("cm_id=%p, change to RDMA_ID_STATE_CONNECTED(%d)\n", cm_id,
         id_state->load());
#endif
  return 0;
}

rdma::Transport::Transport(bool server, const char *server_name,
                           size_t max_num_shards) {
  assert(server == true && "if not serer, specify `size`");
  int ret;
  struct rdma_addrinfo *rai, hints;

  ctx_ = (struct rdma::Context *)malloc(sizeof(struct rdma::Context));
  memset(ctx_, 0, sizeof(rdma::Context));
  memset(&hints, 0, sizeof(struct rdma_addrinfo));
  ctx_->server = server;
  ctx_->server_name = strdup(server_name);
  ctx_->server_port = DEFAULT_PORT;
  ctx_->qp_count = DEFAULT_MAX_QP;
  ctx_->max_wr = DEFAULT_MAX_WR;
  ctx_->msg_length = DEFAULT_MSG_LENGTH;
  ctx_->max_num_shards = max_num_shards;

  hints.ai_port_space = RDMA_PS_TCP;
  if (ctx_->server == true)
    hints.ai_flags = RAI_PASSIVE; /* this makes it a server */
  ret = rdma_getaddrinfo(ctx_->server_name, ctx_->server_port, &hints, &rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }
  ctx_->conn_ids =
      (struct rdma_cm_id **)calloc(ctx_->qp_count, sizeof(struct rdma_cm_id *));
  memset(ctx_->conn_ids, 0, ctx_->qp_count * sizeof(struct rdma_cm_id *));
  ctx_->id_states = new std::atomic<enum rdma_id_state>[ctx_->qp_count];
  for (int i = 0; i < ctx_->qp_count; i++) {
    ctx_->id_states[i] = RDMA_ID_STATE_FREE;
  }
  ctx_->send_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->send_buf, 0, ctx_->msg_length);
  ctx_->recv_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->recv_buf, 0, ctx_->msg_length);

  ctx_->bufs = (char **)calloc(ctx_->qp_count, sizeof(char *));
  ctx_->buf_mrs =
      (struct ibv_mr **)calloc(ctx_->qp_count, sizeof(struct ibv_mr));
  ctx_->num_connnected_client =
      (size_t *)calloc(ctx_->qp_count, sizeof(size_t));

  ret = init_resources(rai);
  if (ret) {
    printf("init_resources returned %d\n", ret);
    return;
  }

  ret = run_server();
}

rdma::Transport::Transport(bool server, const char *server_name, size_t rm_size,
                           size_t shard_id, size_t client_qp_count,
                           size_t local_buf_size) {
  int ret;
  struct rdma_addrinfo *rai, hints;

  assert(server == false && "only client can specify `rm_size`");

  ctx_ = (struct rdma::Context *)malloc(sizeof(struct rdma::Context));
  memset(ctx_, 0, sizeof(rdma::Context));
  memset(&hints, 0, sizeof(struct rdma_addrinfo));
  ctx_->server = server;
  ctx_->shard_id = shard_id;
  ctx_->server_name = strdup(server_name);
  ctx_->server_port = DEFAULT_PORT;
  ctx_->qp_count = client_qp_count;
  ctx_->max_wr = DEFAULT_MAX_WR;
  ctx_->msg_length = DEFAULT_MSG_LENGTH;
  ctx_->local_buf_size = local_buf_size;
  ctx_->rm_size = rm_size;

  hints.ai_port_space = RDMA_PS_TCP;
  if (ctx_->server == true)
    hints.ai_flags = RAI_PASSIVE; /* this makes it a server */
  ret = rdma_getaddrinfo(ctx_->server_name, ctx_->server_port, &hints, &rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }
  ctx_->conn_ids =
      (struct rdma_cm_id **)calloc(ctx_->qp_count, sizeof(struct rdma_cm_id *));
  memset(ctx_->conn_ids, 0, ctx_->qp_count * sizeof(struct rdma_cm_id *));
  ctx_->id_states = new std::atomic<enum rdma_id_state>[ctx_->qp_count];
  for (int i = 0; i < ctx_->qp_count; i++) {
    ctx_->id_states[i] = RDMA_ID_STATE_FREE;
  }
  ctx_->send_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->send_buf, 0, ctx_->msg_length);
  ctx_->recv_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->recv_buf, 0, ctx_->msg_length);

  ctx_->bufs = (char **)calloc(ctx_->qp_count, sizeof(char *));
  for (int i = 0; i < ctx_->qp_count; i++) {
    ctx_->bufs[i] = (char *)malloc(ctx_->local_buf_size);
    memset(ctx_->bufs[i], 0, ctx_->local_buf_size);
  }
  ctx_->buf_mrs =
      (struct ibv_mr **)calloc(ctx_->qp_count, sizeof(struct ibv_mr));

  ret = init_resources(rai);
  if (ret) {
    printf("init_resources returned %d\n", ret);
    return;
  }

  ret = connect_server(rai);
  if (ret) {
    throw std::runtime_error("connect_server failed");
  }
}

rdma::Transport::~Transport() {
  // need to check about memory leak
  destroy_resources();
}

/*
 * Function: init_resources
 *
 * Input:
 * rai The RDMA address info for the connection
 *
 * Description:
 * This function initializes resources that are common to both the client
 * and server functionality.
 * It creates our SRQ, registers memory regions, posts receive buffers
 * and creates a single completion queue that will be used for the receive
 * queue on each queue pair.
 */
int rdma::Transport::init_resources(struct rdma_addrinfo *rai) {
  int ret, i;
  struct rdma_cm_event *cm_event;

  /* create event channel */
  ctx_->event_channel = rdma_create_event_channel();
  if (!ctx_->event_channel) {
    VERB_ERR("rdma_create_event_channel", errno);
    return errno;
  }
  /* Create an ID used for creating/accessing our SRQ */
  ret =
      rdma_create_id(ctx_->event_channel, &ctx_->srq_id, nullptr, RDMA_PS_TCP);
  if (ret) {
    VERB_ERR("rdma_create_id", ret);
    return ret;
  }

  /* We need to bind the ID to a particular RDMA device
   * This is done by resolving the address or binding to the address */
  printf("[%-16s] Resolving/Binding address\n", "Init");
  if (ctx_->server == false) {
    ret = rdma_resolve_addr(ctx_->srq_id, NULL, rai->ai_dst_addr, 1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }
    ret = get_cm_event(ctx_->event_channel, RDMA_CM_EVENT_ADDR_RESOLVED,
                       &cm_event);
    if (ret) {
      VERB_ERR("get_cm_event", ret);
      return ret;
    }
    if (cm_event != nullptr && (ret = rdma_ack_cm_event(cm_event))) {
      VERB_ERR("rdma_ack_cm_event", ret);
      return ret;
    }
  } else {
    ret = rdma_bind_addr(ctx_->srq_id, rai->ai_src_addr);
    if (ret) {
      VERB_ERR("rdma_bind_addr", ret);
      return ret;
    }
  }
  /* Create the memory regions being used in this example */
  printf("[%-16s] Register memory region...\n", "Init");
  ctx_->recv_mr = rdma_reg_msgs(ctx_->srq_id, ctx_->recv_buf, ctx_->msg_length);
  if (!ctx_->recv_mr) {
    VERB_ERR("rdma_reg_msgs recv_mr", -1);
    return -1;
  }
  ctx_->send_mr = rdma_reg_msgs(ctx_->srq_id, ctx_->send_buf, ctx_->msg_length);
  if (!ctx_->send_mr) {
    VERB_ERR("rdma_reg_msgs send_mr", -1);
    return -1;
  }
  if (ctx_->bufs) {
    for (i = 0; i < ctx_->qp_count; i++) {
      if (ctx_->bufs[i]) {
        assert(ctx_->server == false && "only client will init the buf here");
        int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ;
        ctx_->buf_mrs[i] = ibv_reg_mr(ctx_->srq_id->pd, ctx_->bufs[i],
                                      ctx_->local_buf_size, mr_flags);
        if (!ctx_->buf_mrs[i]) {
          VERB_ERR("rdma_reg_msgs buf_mr", -1);
          return -1;
        }
      }
    }
  }
  /* Create our shared receive queue */
  printf("[%-16s] Create shared receive queue...\n", "Init");
  struct ibv_srq_init_attr srq_attr;
  memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = ctx_->max_wr;
  srq_attr.attr.max_sge = 1;
  ret = rdma_create_srq(ctx_->srq_id, NULL, &srq_attr);
  if (ret) {
    VERB_ERR("rdma_create_srq", ret);
    return -1;
  }
  /* Save the SRQ in our context so we can assign it to other QPs later */
  ctx_->srq = ctx_->srq_id->srq;
  /* Post our receive buffers on the SRQ */
  printf("[%-16s] Post recv to SRQ...\n", "Init");
  for (i = 0; i < ctx_->max_wr; i++) {
    ret = rdma_post_recv(ctx_->srq_id, NULL, ctx_->recv_buf, ctx_->msg_length,
                         ctx_->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  }

  /* Create a completion channel to use with the SRQ CQ */
  printf("[%-16s] Create Completion Channel & Completion Queue for SRQ ...\n",
         "Init");
  ctx_->srq_cq_channel = ibv_create_comp_channel(ctx_->srq_id->verbs);
  if (!ctx_->srq_cq_channel) {
    VERB_ERR("ibv_create_comp_channel", -1);
    return -1;
  }
  /* Create a CQ to use for all connections (QPs) that use the SRQ */
  ctx_->srq_cq = ibv_create_cq(ctx_->srq_id->verbs, ctx_->max_wr, NULL,
                               ctx_->srq_cq_channel, 0);
  if (!ctx_->srq_cq) {
    VERB_ERR("ibv_create_cq", -1);
    return -1;
  }
  /* Make sure that we get notified on the first completion */
  printf("[%-16s] Make SRQ req notify ...\n", "Init");
  ret = ibv_req_notify_cq(ctx_->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

/*
 * Function: destroy_resources
 *
 * Description:
 * This function cleans up resources used by the application
 */
void rdma::Transport::destroy_resources() {
  int i;
  if (ctx_->conn_ids && not ctx_->server) {
    for (i = 0; i < ctx_->qp_count; i++) {
      uint32_t qp_num = ctx_->conn_ids[i]->qp->qp_num;
      if (ctx_->conn_ids[i] && ctx_->conn_ids[i]->qp &&
          ctx_->conn_ids[i]->qp->state == IBV_QPS_RTS) {
        rdma_disconnect(ctx_->conn_ids[i]);
      }
      rdma_destroy_qp(ctx_->conn_ids[i]);
      rdma_destroy_id(ctx_->conn_ids[i]);
      printf("[%-16s] connection %d (qp_num=%u) destroyed\n", "Info", i,
             qp_num);
    }
    free(ctx_->conn_ids);
  }
  if (ctx_->id_states) {
    free(ctx_->id_states);
  }
  if (ctx_->recv_mr)
    rdma_dereg_mr(ctx_->recv_mr);
  if (ctx_->send_mr)
    rdma_dereg_mr(ctx_->send_mr);
  if (ctx_->buf_mrs) {
    for (i = 0; i < ctx_->qp_count; i++) {
      if (ctx_->buf_mrs[i])
        rdma_dereg_mr(ctx_->buf_mrs[i]);
    }
    free(ctx_->buf_mrs);
  }
  if (ctx_->recv_buf)
    free(ctx_->recv_buf);
  if (ctx_->send_buf)
    free(ctx_->send_buf);
  if (ctx_->bufs) {
    for (i = 0; i < ctx_->qp_count; i++) {
      if (ctx_->bufs[i])
        free(ctx_->bufs[i]);
    }
    free(ctx_->bufs);
  }
  if (ctx_->srq_cq)
    ibv_destroy_cq(ctx_->srq_cq);
  if (ctx_->event_channel)
    rdma_destroy_event_channel(ctx_->event_channel);
  if (ctx_->srq_cq_channel)
    ibv_destroy_comp_channel(ctx_->srq_cq_channel);
  if (ctx_->srq_id) {
    rdma_destroy_srq(ctx_->srq_id);
    rdma_destroy_id(ctx_->srq_id);
  }
  if (ctx_->server_name) {
    free(ctx_->server_name);
  }
  if (ctx_->num_connnected_client) {
    free(ctx_->num_connnected_client);
  }
  delete ctx_;
}

/*
 * Function: await_completion
 *
 * Description:
 * Waits for a completion on the SRQ CQ
 *
 */
int rdma::Transport::await_completion() {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;
  /* Wait for a CQ event to arrive on the channel */
  ret = ibv_get_cq_event(ctx_->srq_cq_channel, &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  /* Reload the event notification */
  ret = ibv_req_notify_cq(ctx_->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }
  return 0;
}

/*
 * Function: run_server
 *
 */
int rdma::Transport::run_server() {
  int ret;
  struct rdma_cm_event *cm_event;

  /* Use the srq_id as the listen_id since it is already setup */
  ctx_->listen_id = ctx_->srq_id;
  ret = rdma_listen(ctx_->listen_id, 4);
  if (ret) {
    VERB_ERR("rdma_listen", ret);
    return ret;
  }
  printf("[%-16s] waiting for connection from client...\n", "Info");

  while (true) {
    // TODO: handle all kinds of event (e.g., disconnect.)
    ret = rdma_get_cm_event(ctx_->event_channel, &cm_event);
    if (ret) {
      VERB_ERR("rdma_get_cm_event", ret);
      return ret;
    }
    printf("[%-16s] got cm_event: %s (%d)\n", "CM Event",
           rdma_event_str(cm_event->event), cm_event->event);
    if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      handle_connect_request(cm_event);
      ret = rdma_ack_cm_event(cm_event);
      if (ret) {
        VERB_ERR("rdma_ack_cm_event", ret);
        return ret;
      }
    } else if (cm_event->event == RDMA_CM_EVENT_ESTABLISHED) {
      ret = rdma_ack_cm_event(cm_event);
      if (ret) {
        VERB_ERR("rdma_ack_cm_event", ret);
        return ret;
      }
    } else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
      handle_disconnected(cm_event);
    } else if (cm_event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
      handle_timewait_exit(cm_event);
    }
  }
}

/*
 * Function: connect_server
 */
int rdma::Transport::connect_server(struct rdma_addrinfo *rai) {
  int ret;
  char *ptr;
  struct ibv_qp_init_attr attr;

  assert(ctx_->server == false);

  for (int i = 0; i < ctx_->qp_count; i++) {
    std::lock_guard<std::mutex> lock(mutex_);
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = ctx_;
    attr.cap.max_send_wr = ctx_->max_wr;
    attr.cap.max_recv_wr = ctx_->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = ctx_->srq_cq;
    attr.srq = ctx_->srq;
    attr.sq_sig_all = 0;

    // create endpoint
    ret = rdma_create_ep(&ctx_->conn_ids[i], rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_ep", ret);
      return ret;
    }
    // send connect request
    printf("[%-16s] connecting... (ctx_->conn_ids[%d]=%p)\n", "Info", i,
           ctx_->conn_ids[i]);
    ret = rdma_connect(ctx_->conn_ids[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }

    ctx_->id_states[i] = RDMA_ID_STATE_CONNECTED;
    qp_num_2_conn_idx.insert(std::make_pair(ctx_->conn_ids[i]->qp->qp_num, i));

    // TODO: this information exchange maybe redundant (in the loop)
    // send requested remote memory information
    ptr = ctx_->send_buf;
    memset(ptr, 0, ctx_->msg_length);
    memcpy(ptr, &ctx_->shard_id, sizeof(ctx_->shard_id));
    ptr += sizeof(ctx_->shard_id);
    memcpy(ptr, &ctx_->rm_size, sizeof(ctx_->rm_size));
    ret = send(ctx_->conn_ids[i], ctx_->send_buf, ctx_->msg_length,
               ctx_->send_mr);
    if (ret) {
      VERB_ERR("send", ret);
      return ret;
    }

    // recv remote memory information
    ret = recv(ctx_->conn_ids[i], ctx_->recv_buf, ctx_->msg_length,
               ctx_->recv_mr);
    if (ret) {
      VERB_ERR("recv", ret);
      return ret;
    }
    printf("[%-16s] connected! (ctx_->conn_ids[%d]=%p, qp_num=%u)\n", "Info", i,
           ctx_->conn_ids[i], ctx_->conn_ids[i]->qp->qp_num);

    // TODO: DEBUG: wzh: here is an bug
    if (i == 0) {
      ptr = ctx_->recv_buf;
      memcpy(&ctx_->rm_rkey, ptr, sizeof(ctx_->rm_rkey));
      ptr += sizeof(ctx_->rm_rkey);
      memcpy(&ctx_->rm_addr, ptr, sizeof(ctx_->rm_addr));
      ptr += sizeof(ctx_->rm_addr);
    }
    printf(
        "[%-16s] remote memory info: shard_id: %lu, rkey: 0x%x, addr: 0x%lx, "
        "size: %lu\n",
        "Info", ctx_->shard_id, ctx_->rm_rkey, ctx_->rm_addr, ctx_->rm_size);
    assert(ctx_->rm_addr);
  }

  return 0;
}

/*
 * Function: handle_connect_request
 *
 * TODO: reject if all conn_ids are connected
 */
int rdma::Transport::handle_connect_request(struct rdma_cm_event *cm_event) {
  int ret;
  struct ibv_qp_init_attr qp_attr;
  struct rdma_cm_id *conn_id;
  char *ptr;

  // TODO: return error instead of assert
  int next_conn_ids_idx = 0;
  while (next_conn_ids_idx < ctx_->qp_count &&
         ctx_->id_states[next_conn_ids_idx] != RDMA_ID_STATE_FREE) {
    next_conn_ids_idx++;
  }
  if (next_conn_ids_idx == ctx_->qp_count) {
    printf("[%-16s] failed to connect, all conn_ids are connected\n",
           "Connection");
    rdma_reject(cm_event->id, NULL, 0);
    return -1;
  }

  /* Create the queue pair */
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_context = ctx_;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = ctx_->max_wr;
  qp_attr.cap.max_recv_wr = ctx_->max_wr;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 0;
  qp_attr.recv_cq = ctx_->srq_cq;
  qp_attr.srq = ctx_->srq;
  qp_attr.sq_sig_all = 0;
  {
    // NOTE: don't move this lock block, it won't hurt performance because this
    // is only used in initializing stage
    std::lock_guard<std::mutex> lock(mutex_);
    assert(ctx_->id_states[next_conn_ids_idx] == RDMA_ID_STATE_FREE);
    ctx_->conn_ids[next_conn_ids_idx] = cm_event->id;
    conn_id = ctx_->conn_ids[next_conn_ids_idx];
    ret = rdma_create_qp(conn_id, NULL, &qp_attr);
    printf("[%-16s] create qp_num: %u\n", "Connection", conn_id->qp->qp_num);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }
    /* Set the new connection to use our SRQ */
    conn_id->srq = ctx_->srq;
    ret = rdma_accept(conn_id, NULL);
    if (ret) {
      VERB_ERR("rdma_accept", ret);
      return ret;
    }

    ctx_->id_states[next_conn_ids_idx] = RDMA_ID_STATE_CONNECTED;
    qp_num_2_conn_idx.insert(
        std::make_pair(conn_id->qp->qp_num, next_conn_ids_idx));
    ++next_conn_ids_idx;

    // recv requested remote memory information
    ret = recv(conn_id, ctx_->recv_buf, ctx_->msg_length, ctx_->recv_mr);
    if (ret) {
      VERB_ERR("recv", ret);
      return ret;
    }
    ptr = ctx_->recv_buf;

    size_t shard_id, rm_size;
    memcpy(&shard_id, ptr, sizeof(shard_id));
    if (shard_id >= ctx_->max_num_shards) {
      printf("shard_id=%lu, max_num_shards=%lu\n", shard_id, ctx_->max_num_shards);
      throw std::runtime_error("shard_id is out of range");
    }
    ptr += sizeof(sizeof(shard_id));
    memcpy(&rm_size, ptr, sizeof(rm_size));
    qp_num_2_shard_id.insert(
        std::make_pair(conn_id->qp->qp_num, next_conn_ids_idx));

    // allocate remote memory
    if (!ctx_->bufs[shard_id]) {
      ctx_->bufs[shard_id] = (char *)malloc(rm_size);
      memset(ctx_->bufs[shard_id], 0, rm_size);
      int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ;
      ctx_->buf_mrs[shard_id] =
          ibv_reg_mr(ctx_->srq_id->pd, ctx_->bufs[shard_id], rm_size, mr_flags);
      if (!ctx_->buf_mrs[shard_id]) {
        VERB_ERR("rdma_reg_msgs buf_mr", -1);
        return -1;
      }
      ctx_->num_connnected_client[shard_id]++;
      assert(rm_size == ctx_->buf_mrs[shard_id]->length);
    } else {
      assert(ctx_->bufs[shard_id] && ctx_->buf_mrs[shard_id]);
    }

    // send back remote memory information
    ptr = ctx_->send_buf;
    memset(ptr, 0, ctx_->msg_length);
    memcpy(ptr, &ctx_->buf_mrs[shard_id]->rkey, sizeof(ctx_->rm_rkey));
    ptr += sizeof(ctx_->rm_rkey);
    uint64_t addr = (uint64_t)ctx_->buf_mrs[shard_id]->addr;
    memcpy(ptr, &addr, sizeof(ctx_->rm_addr));

    ret = send(conn_id, ctx_->send_buf, ctx_->msg_length, ctx_->send_mr);
    if (ret) {
      VERB_ERR("send", ret);
      return ret;
    }

    printf(
        "[%-16s] sent rm info to %u (rkey=0x%x, rm_addr=0x%lx, rm_size=%lu)\n",
        "Info", conn_id->qp->qp_num, ctx_->buf_mrs[shard_id]->rkey, addr,
        ctx_->buf_mrs[shard_id]->length);
  }

  return 0;
}

/*
 * Function: handle_disconnected
 *
 */
int rdma::Transport::handle_disconnected(struct rdma_cm_event *cm_event) {
  int ret;
  struct rdma_cm_id *conn_id = cm_event->id;
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    VERB_ERR("rdma_ack_cm_event", ret);
    return ret;
  }

  uint32_t qp_num = conn_id->qp->qp_num;
  if (conn_id->qp && conn_id->qp->state == IBV_QPS_RTS) {
    ret = rdma_disconnect(conn_id);
    if (ret) {
      VERB_ERR("rdma_disconnect", ret);
      return ret;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      assert(ctx_->id_states[qp_num_2_conn_idx[qp_num]] =
                 RDMA_ID_STATE_CONNECTED);
      ctx_->id_states[qp_num_2_conn_idx[qp_num]] = RDMA_ID_STATE_DISCONNECTED;
      printf("[%-16s] disconnected %u\n", "Connection", qp_num);
    }
  }

  return 0;
}

/*
 * Function: handle_timewait_exit
 *
 */
int rdma::Transport::handle_timewait_exit(struct rdma_cm_event *cm_event) {
  int ret;
  struct rdma_cm_id *conn_id = cm_event->id;
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    VERB_ERR("rdma_ack_cm_event", ret);
    return ret;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    uint32_t qp_num = conn_id->qp->qp_num;
    size_t conn_idx = qp_num_2_conn_idx[qp_num];
    assert(ctx_->id_states[qp_num_2_conn_idx[qp_num]] ==
           RDMA_ID_STATE_DISCONNECTED);
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_TIMEWAIT_EXITED;
    rdma_destroy_qp(conn_id);
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_QP_DESTROYED;
    rdma_destroy_id(conn_id);
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_ID_DESTROYED;
    printf("[%-16s] disconnected! (qp_num=%u)\n", "Connection", qp_num);
    ctx_->conn_ids[conn_idx] = nullptr;
    qp_num_2_conn_idx.erase(qp_num);
    size_t shard_id = qp_num_2_shard_id[qp_num];
    ctx_->num_connnected_client[shard_id]--;
    // TODO: should free here
    // assert(ctx_->bufs[shard_id]);
    // if (ctx_->num_connnected_client[shard_id] == 0) {
    //   free(ctx_->bufs[shard_id]);
    //   ibv_dereg_mr(ctx_->buf_mrs[shard_id]);
    //   ctx_->buf_mrs[shard_id] = nullptr;
    // }
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_FREE;
    printf("[%-16s] freed! (qp_num=%u)\n", "Connection", qp_num);
  }

  return 0;
}

/*
 * Function: read_rm
 *
 * Input:
 */
int rdma::Transport::read_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                             struct ibv_mr *lm_mr, uint64_t rm_addr,
                             uint32_t rm_rkey,
                             RDMAAsyncRequest *async_request) {
  int ret, ne;
  struct ibv_wc wc = {};

#ifdef CHECK_RDMA_CM_ID_WORKING
  size_t conn_idx = qp_num_2_conn_idx[cm_id->qp->qp_num];
  enum rdma::rdma_id_state connected_state = RDMA_ID_STATE_READING;
  while (!ctx_->id_states[conn_idx].compare_exchange_weak(
      connected_state, RDMA_ID_STATE_READING))
    ;
  printf("cm_id=%p, change to RDMA_ID_STATE_READING(%d)\n", cm_id,
         ctx_->id_states[conn_idx].load());
#endif

  ret = rdma_post_read(cm_id, /*context=*/nullptr, lm_addr, lm_length, lm_mr,
                       /*flags=*/IBV_SEND_SIGNALED, rm_addr, rm_rkey);
  if (ret) {
    VERB_ERR("rdma_post_read", ret);
    throw(std::runtime_error("rdma_post_read failed"));
    return ret;
  }
  assert(ctx_->rm_addr <= rm_addr);
  assert(rm_addr + lm_length <= ctx_->rm_addr + ctx_->rm_size);
  // printf(
  //     "[%-16s] post read: rkey=0x%x, length=%lu, , region [0x%lu, 0x%lu) in "
  //     "[0x%lu, 0x%lu), async=%p\n",
  //     "Info", rm_rkey, lm_length, rm_addr, rm_addr + lm_length,
  //     ctx_->rm_addr, ctx_->rm_addr + ctx_->rm_size, async_request);

  if (async_request != nullptr) {
#ifdef CHECK_RDMA_CM_ID_WORKING
    async_request->id_state = &ctx_->id_states[conn_idx];
#endif
    async_request->type = ASYNC_READ;
    async_request->cm_id = cm_id;
    async_request->lm_addr = lm_addr;
    async_request->lm_length = lm_length;
    async_request->rm_addr = rm_addr;
  } else {
    do {
      ne = ibv_poll_cq(cm_id->send_cq, 1, &wc);
      if (ne < 0) {
        VERB_ERR("ibv_poll_cq", ne);
        throw std::runtime_error("ibv_poll_cq failed");
        return ne;
      }

      if (wc.status != IBV_WC_SUCCESS) {
        VERB_ERR("ibv_poll_cq", wc.status);
        throw std::runtime_error("ibv_poll_cq failed");
        return wc.status;
      }
    } while (ne == 0);

#ifdef CHECK_RDMA_CM_ID_WORKING
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_CONNECTED;
    printf("cm_id=%p, change to RDMA_ID_STATE_CONNECTED(%d)\n", cm_id,
           ctx_->id_states[conn_idx].load());
#endif
  }

  return 0;
}

/*
 * Function: write_rm
 *
 * Input:
 */
int rdma::Transport::write_rm(rdma_cm_id *cm_id, char *lm_addr,
                              size_t lm_length, struct ibv_mr *lm_mr,
                              uint64_t rm_addr, uint32_t rm_rkey,
                              RDMAAsyncRequest *async_request) {
  int ret, ne;
  struct ibv_wc wc = {};

#ifdef CHECK_RDMA_CM_ID_WORKING
  size_t conn_idx = qp_num_2_conn_idx[cm_id->qp->qp_num];
  assert(conn_idx == 1);
  enum rdma::rdma_id_state connected_state = RDMA_ID_STATE_WRITING;
  while (!ctx_->id_states[conn_idx].compare_exchange_weak(
      connected_state, RDMA_ID_STATE_WRITING))
    ;
  printf("cm_id=%p, change to RDMA_ID_STATE_WRITING(%d)\n", cm_id,
         ctx_->id_states[conn_idx].load());
#endif

  ret = rdma_post_write(cm_id, /*context=*/nullptr, lm_addr, lm_length, lm_mr,
                        /*flags=*/IBV_SEND_SIGNALED, rm_addr, rm_rkey);
  if (ret) {
    VERB_ERR("rdma_post_write", ret);
    return ret;
  }
  assert(ctx_->rm_addr <= rm_addr);
  assert(rm_addr + lm_length <= ctx_->rm_addr + ctx_->rm_size);
  // printf(
  //     "[%-16s] post write: rkey=0x%x, length=%lu, region [0x%lu, 0x%lu) in "
  //     "[0x%lu, 0x%lu), async=%p\n",
  //     "Info", rm_rkey, lm_length, rm_addr, rm_addr + lm_length,
  //     ctx_->rm_addr, ctx_->rm_addr + ctx_->rm_size, async_request);

  if (async_request != nullptr) {
#ifdef CHECK_RDMA_CM_ID_WORKING
    async_request->id_state = &ctx_->id_states[conn_idx];
#endif
    async_request->type = ASYNC_WRITE;
    async_request->cm_id = cm_id;
    async_request->lm_addr = lm_addr;
    async_request->lm_length = lm_length;
    async_request->rm_addr = rm_addr;
  } else {
    do {
      ne = ibv_poll_cq(cm_id->send_cq, 1, &wc);
      if (ne < 0) {
        VERB_ERR("ibv_poll_cq", ne);
        return ne;
      }

      if (wc.status != IBV_WC_SUCCESS) {
        VERB_ERR("ibv_poll_cq", wc.status);
        return wc.status;
      }
    } while (ne == 0);

#ifdef CHECK_RDMA_CM_ID_WORKING
    ctx_->id_states[conn_idx] = RDMA_ID_STATE_CONNECTED;
    printf("cm_id=%p, change to RDMA_ID_STATE_CONNECTED(%d)\n", cm_id,
           ctx_->id_states[conn_idx].load());
#endif
  }

  return 0;
}

/*
 * Function: send
 *
 * Input:
 */
int rdma::Transport::send(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                          struct ibv_mr *send_mr) {
  int ret, ne;
  struct ibv_wc wc = {};

  ret = rdma_post_send(cm_id, /*context=*/nullptr, lm_addr, lm_length, send_mr,
                       IBV_SEND_SIGNALED);
  if (ret) {
    VERB_ERR("rdma_post_send", ret);
    return ret;
  }
  do {
    ne = rdma_get_send_comp(cm_id, &wc);
    if (ne < 0) {
      VERB_ERR("rdma_get_send_comp", ret);
      return ret;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      VERB_ERR("rdma_get_send_comp", wc.status);
      return wc.status;
    }
  } while (ne == 0);

  return 0;
}

/*
 * Function: recv
 *
 * Input:
 */
int rdma::Transport::recv(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                          struct ibv_mr *recv_mr) {
  int ret, ne;
  struct ibv_wc wc = {};

  memset(ctx_->recv_buf, 0, ctx_->msg_length);
  ret = await_completion();
  if (ret) {
    printf("await_completion %d\n", ret);
    return ret;
  }
  do {
    ne = ibv_poll_cq(ctx_->srq_cq, 1, &wc);
    if (ne < 0) {
      VERB_ERR("ibv_poll_cq", ne);
      return ne;
    } else if (ne == 0)
      break;
    if (wc.status != IBV_WC_SUCCESS) {
      printf("work completion status %s\n", ibv_wc_status_str(wc.status));
      return -1;
    }

    ret = rdma_post_recv(ctx_->srq_id, (void *)wc.wr_id, ctx_->recv_buf,
                         ctx_->msg_length, ctx_->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  } while (ne);

  return 0;
}
