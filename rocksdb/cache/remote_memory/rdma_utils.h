#pragma once

#include <infiniband/verbs.h>
#include <rdma/rdma_verbs.h>
#include <iostream>

namespace rdma {
void print_cm_id(struct rdma_cm_id *id);
void print_string_as_hex(char *str, int len);
std::string char_array_to_string(char *array, int size);

int get_cm_event(struct rdma_event_channel *channel,
                 enum rdma_cm_event_type type, struct rdma_cm_event **out_ev);
}; // namespace rdma