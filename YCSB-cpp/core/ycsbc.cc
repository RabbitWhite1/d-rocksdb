//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <sched.h>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "core_workload.h"
#include "countdown_latch.h"
#include "db_factory.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[],
                      ycsbc::utils::Properties &props);
void DoTransaction(const int total_ops, const int num_threads,
                   ycsbc::Measurements &measurements, ycsbc::DB *status_db,
                   const bool show_status, const int status_interval,
                   std::vector<ycsbc::DB *> &dbs, ycsbc::CoreWorkload &wl,
                   const bool do_load, const bool is_warmup);

void StatusThread(ycsbc::DB *db, ycsbc::Measurements *measurements,
                  CountDownLatch *latch, int interval, bool *has_warmup_done,
                  bool reset_stats) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;

  int stable_times = 0;
  float prev_hit_ratio = 0.0;
  float prev_mv_hit_ratio = 0.0;
  float mv_hit_ratio = 0.0;
  printf("Status thread start.\n");

  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    char time_str[100];
    std::strftime(time_str, sizeof(time_str), "%F %T", std::localtime(&now_c));
    printf("\033[1;32m%s\033[0m %lld sec: %s\n", time_str,
           static_cast<long long>(elapsed_time.count()),
           measurements->GetStatusMsg().c_str());

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);

    if (db != nullptr) {
      db->GetOrPrintDBStatus(nullptr, /*should_print=*/true,
                             /*reset_stats=*/false);
      if (has_warmup_done != nullptr and *has_warmup_done == false) {
        static std::string metric_name = "block_cache_hit_ratio";

        std::map<std::string, std::string> status_map;
        db->GetOrPrintDBStatus(&status_map, /*should_print=*/false,
                               /*reset_stats=*/true);
        float current_hit_ratio = std::stof(status_map[metric_name]);

        if (std::isnan(current_hit_ratio)) {
          // still need to wait so that it won't output too many times.
          latch->AwaitFor(interval);
          continue;
        }
        if (std::abs(current_hit_ratio - mv_hit_ratio) <= 3) {
          ++stable_times;
        } else {
          stable_times = 0;
        }
        printf("metric=%s: mv_hit_ratio=%.4f%%, prev_hit_ratio=%.4f%%, "
               "current_hit_ratio=%.4f%%, delta_hit_ratio=%.4f%%, "
               "stable_times=%d\n",
               metric_name.c_str(), mv_hit_ratio, prev_hit_ratio,
               current_hit_ratio, std::abs(current_hit_ratio - mv_hit_ratio),
               stable_times);
        prev_mv_hit_ratio = mv_hit_ratio;
        mv_hit_ratio = (mv_hit_ratio + current_hit_ratio) / 2;
        prev_hit_ratio = current_hit_ratio;
        if (stable_times >= 7) {
          *has_warmup_done = true;
          break;
        }
      }
    }
    fflush(stdout);
  }
}

int main(const int argc, const char *argv[]) {
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction =
      (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  ycsbc::Measurements measurements;
  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads + 1; i++) {
    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, &measurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }
  ycsbc::DB *status_db = dbs[num_threads];
  status_db->Init();

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval =
      std::stoi(props.GetProperty("status.interval", "10"));

  // load phase
  if (do_load) {
    const int total_ops =
        stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(
          std::launch::async, StatusThread, status_db, &measurements, &latch,
          status_interval, /*has_warmup_done=*/nullptr, /*reset_stats=*/true);
    }
    // client threads
    unsigned num_cpus = std::thread::hardware_concurrency();
    assert(num_cpus >= num_threads);
    std::vector<std::promise<int>> client_return_promises;
    std::vector<std::future<int>> client_return_futures;
    for (int i = 0; i < num_threads; ++i) {
      client_return_promises.emplace_back(std::promise<int>());
    }
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      client_return_futures.emplace_back(
          client_return_promises[i].get_future());
      std::thread client_thread = std::thread(
          ycsbc::ClientThread, dbs[i], &wl, thread_ops, /*is_loading=*/true,
          /*init_db=*/true, /*cleanup_db=*/!do_transaction, &latch,
          std::ref(client_return_promises[i]), i,
          /*is_warmup=*/false, /*has_warmup_done=*/nullptr);
      int cpu_to_use = i;
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(cpu_to_use, &cpuset);
      int rc = pthread_setaffinity_np(client_thread.native_handle(),
                                      sizeof(cpu_set_t), &cpuset);
      if (rc != 0) {
        std::cerr << "trying to use cpu: " << cpu_to_use << std::endl;
        std::cerr << "num cpus: " << num_cpus << std::endl;
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
      }
      client_thread.detach();
    }
    assert((int)client_return_futures.size() == num_threads);

    int sum = 0;
    for (auto &n : client_return_futures) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << sum << std::endl;
    std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
  }

  measurements.Reset();
  std::this_thread::sleep_for(
      std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));

  // transaction phase
  if (do_transaction) {
    const int total_ops =
        stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    const int total_warmup_ops = -1;
    printf("Warmup starts! Total warmup ops: %d; %s\n", total_warmup_ops,
           measurements.GetStatusMsg().c_str());
    DoTransaction(total_warmup_ops, num_threads, measurements, status_db,
                  show_status, status_interval, dbs, wl, do_load,
                  /*is_warmup=*/true);
    printf("Warmup done! %s\n", measurements.GetStatusMsg().c_str());
    measurements.Reset();
    status_db->GetOrPrintDBStatus(nullptr, /*should_print=*/false,
                                  /*reset_stats=*/true);
    printf("measurements reset! %s\n", measurements.GetStatusMsg().c_str());
    printf("\033[1;31mStart doing the required transactions! (%d ops)\033[0m\n", total_ops);
    printf("Press any key to continue...\n");
    fflush(stdout);
    // getchar();
    DoTransaction(total_ops, num_threads, measurements, status_db, show_status,
                  status_interval, dbs, wl, do_load, /*is_warmup=*/false);
  }
  status_db->Cleanup();
  for (int i = 0; i < num_threads + 1; i++) {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, const char *argv[],
                      ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 ||
               strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string &message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)"
                  << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  std::cout
      << "Usage: " << command
      << " [options]\n"
         "Options:\n"
         "  -load: run the loading phase of the workload\n"
         "  -t: run the transactions phase of the workload\n"
         "  -run: same as -t\n"
         "  -threads n: execute using n threads (default: 1)\n"
         "  -db dbname: specify the name of the DB to use (default: basic)\n"
         "  -P propertyfile: load properties from the given file. Multiple "
         "files can\n"
         "                   be specified, and will be processed in the order "
         "specified\n"
         "  -p name=value: specify a property to be passed to the DB and "
         "workloads\n"
         "                 multiple properties can be specified, and override "
         "any\n"
         "                 values in the propertyfile\n"
         "  -s: print status every 10 seconds (use status.interval prop to "
         "override)"
      << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void DoTransaction(const int total_ops, const int num_threads,
                   ycsbc::Measurements &measurements, ycsbc::DB *status_db,
                   const bool show_status, const int status_interval,
                   std::vector<ycsbc::DB *> &dbs, ycsbc::CoreWorkload &wl,
                   const bool do_load, const bool is_warmup) {
  CountDownLatch latch(num_threads);
  ycsbc::utils::Timer<double> timer;

  timer.Start();
  std::future<void> status_future;
  bool *has_warmup_done = nullptr;
  if (is_warmup) {
    has_warmup_done = new bool(false);
  }
  if (show_status) {
    bool reset_stats = is_warmup;
    status_future =
        std::async(std::launch::async, StatusThread, status_db, &measurements,
                   &latch, status_interval, has_warmup_done, reset_stats);
  }
  std::cout << "started staus thread" << std::endl;
  // client threads
  unsigned num_cpus = std::thread::hardware_concurrency();
  std::vector<std::promise<int>> client_return_promises;
  std::vector<std::future<int>> client_return_futures;
  for (int i = 0; i < num_threads; ++i) {
    client_return_promises.emplace_back(std::promise<int>());
  }
  for (int i = 0; i < num_threads; ++i) {
    int thread_ops = -1;
    if (is_warmup && total_ops == -1) {
      thread_ops = -1;
    } else {
      thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
    }
    client_return_futures.emplace_back(client_return_promises[i].get_future());
    std::thread client_thread = std::thread(
        ycsbc::ClientThread, dbs[i], &wl, thread_ops, /*is_loading=*/false,
        /*init_db=*/true, /*cleanup_db=*/true, &latch,
        std::ref(client_return_promises[i]), i,
        /*is_warmup=*/has_warmup_done != nullptr, has_warmup_done);
    int cpu_to_use = i;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_to_use, &cpuset);
    int rc = pthread_setaffinity_np(client_thread.native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    client_thread.detach();
  }
  assert((int)client_return_futures.size() == num_threads);
  printf("Client threads started\n");

  int sum = 0;
  for (auto &n : client_return_futures) {
    assert(n.valid());
    sum += n.get();
  }
  double runtime = timer.End();

  if (show_status) {
    status_future.wait();
  }

  std::cout << "Run runtime(sec): " << runtime << std::endl;
  std::cout << "Run operations(ops): " << sum << std::endl;
  std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;

  delete has_warmup_done;
}