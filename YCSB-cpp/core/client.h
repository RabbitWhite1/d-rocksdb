//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <future>
#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include "countdown_latch.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, CountDownLatch *latch, std::promise<int> &return_promise, int thread_id,
                        bool is_warmup, bool* has_warmup_done) {
  // Parameters:
  //    db: The database to use.
  //    wl: The workload to use.
  //    num_ops: The number of operations to run; if it is -1, then we warmup until cache usage is high enough.
  if (init_db) {
    db->Init();
  }
  
  int oks = 0;
  int using_num_ops = num_ops;
  if (num_ops == -1) {
    using_num_ops = std::numeric_limits<int>::max();
  }
  if (thread_id == 0) {
    printf("num_ops: %d, using_num_ops: %d\n", num_ops, using_num_ops);
  }
  
  for (int i = 0; i < using_num_ops; ++i) {
    if (is_loading) {
      oks += wl->DoInsert(*db);
    } else {
      oks += wl->DoTransaction(*db);
    }
    if (is_warmup && has_warmup_done != nullptr && *has_warmup_done == true) {
      break;
    }
  }

  if (cleanup_db) {
    db->Cleanup();
  }

  latch->CountDown();
  return_promise.set_value(oks);
  return oks;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
