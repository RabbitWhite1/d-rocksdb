from nbformat import write
import yaml
import os
import os.path as osp
from exprmngr import ExprMngr
import sys
import re
import pandas as pd
import numpy as np



CODE_DIR = osp.dirname(__file__)
if CODE_DIR not in sys.path:
    sys.path.append(CODE_DIR)


ROCKSDB_PROPERTIES = """
rocksdb.dbname={dbname}
rocksdb.format=single
rocksdb.destroy=false

# Load options from file
#rocksdb.optionsfile=rocksdb/options.ini

# Below options are ignored if options file is used
rocksdb.compression=no
rocksdb.max_background_jobs={max_background_jobs}
rocksdb.target_file_size_base=67108864
rocksdb.target_file_size_multiplier=1
rocksdb.max_bytes_for_level_base=268435456
rocksdb.write_buffer_size={write_buffer_size}
rocksdb.max_open_files=-1
rocksdb.max_write_buffer_number={max_write_buffer_number}
rocksdb.min_write_buffer_number_to_merge={min_write_buffer_number_to_merge}
rocksdb.use_direct_io_for_flush_compaction=false
rocksdb.use_direct_reads=false
rocksdb.allow_mmap_writes=false
rocksdb.allow_mmap_reads=false
rocksdb.cache_size={cache_size}
rocksdb.rm_ratio={rm_ratio}
rocksdb.table_cache_numshardbits={table_cache_numshardbits}
rocksdb.compressed_cache_size=0
rocksdb.bloom_bits=0

rocksdb.increase_parallelism=false
rocksdb.optimize_level_style_compaction=false
"""
WORKLOADA = """
recordcount={recordcount}
operationcount={operationcount}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0

requestdistribution=zipfian
"""
WORKLOADB = """
recordcount={recordcount}
operationcount={operationcount}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0

requestdistribution=zipfian
"""
WORKLOADC = """
recordcount={recordcount}
operationcount={operationcount}
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution={requestdistribution}
requestdistribution_zipfian_alpha={zipfian_alpha}
"""

LOAD_CMD = 'date && time ./ycsb -load -db rocksdb -threads {threads} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/workload/{conf_id} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/rocksdb_properties/{conf_id}.properties -s '\
'> {log_path} '
RUN_WITH_INIT_CMD = 'date && time rm -rf /home/wzh/nvme/ycsb-rocksdb && time cp -r /home/wzh/nvme/ycsb-rocksdb.template /home/wzh/nvme/ycsb-rocksdb && time ./ycsb -run -db rocksdb -threads {threads} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/workload/{conf_id} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/rocksdb_properties/{conf_id}.properties -s '\
'> {log_path}'
RUN_WITHOUT_INIT_CMD = 'date && time ./ycsb -run -db rocksdb -threads {threads} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/workload/{conf_id} '\
'-P /home/wzh/d-rocksdb/ycsb_expr/rocksdb_properties/{conf_id}.properties -s '\
'|tee {log_path}'

DBNAMES = {'a': '/home/spdk/nvme/wzh/ycsb-rocksdb', 'b': '/home/spdk/nvme/wzh/ycsb-rocksdb', 'c': '/home/spdk/nvme/wzh/ycsb-rocksdb.readonly'}
WORKLOADS = {'a': WORKLOADA, 'b': WORKLOADB, 'c': WORKLOADC}
RUN_CMDS = {'a': RUN_WITH_INIT_CMD, 'b': RUN_WITH_INIT_CMD, 'c': RUN_WITHOUT_INIT_CMD}
DB_DIRNAME = osp.dirname(__file__)
DB_PATH = osp.join(osp.dirname(__file__), 'log.sqlite')


def parse_log(log_path):
    with open(log_path) as f:
        log_content = f.read()
    try:
        log_lines = log_content.split('\n')
        result = {}
        result['throughput'] = float(re.match(r'Run throughput\(ops/sec\): (\d+(.\d*){0,1}(e\+\d+){0,1})', log_lines[-2]).group(1))
        result['runtime'] = float(re.match(r'Run runtime\(sec\): (\d+(.\d*){0,1}(e\+\d+){0,1})', log_lines[-4]).group(1))
    except:
        # print(log_content)
        raise RuntimeError(f'failed to parse {log_path=}')
    return result


if __name__ == '__main__':
    table_def = yaml.load(open(osp.join(DB_DIRNAME, 'etc/table_def.yaml')).read(), Loader=yaml.SafeLoader)
    config = yaml.load(open(osp.join(DB_DIRNAME, 'etc/config.yaml')).read(), Loader=yaml.SafeLoader)
    mngr = ExprMngr(table_def, DB_PATH, config, autoupdate=True)
    conf_id_list = []
    command_list = []
    zipfian_alpha = 0.99
    requestdistribution = 'zipfian'
    max_write_buffer_number = 4
    max_background_jobs = 8
    table_cache_numshardbits = 4
    # for version in ['no_init0', 'no_init1', 'no_init2', 'no_init3', 'no_init4']:
    #     workload = 'b'
    #     operationcount = 1000000
    #     write_buffer_size = 1 * 1024**3
    #     cache_size = 4 * 1024**3
    #     threads = 32
    #     recordcount = 134217728
    #     conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution, max_write_buffer_number=max_write_buffer_number)
    #     rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size, max_write_buffer_number=max_write_buffer_number)
    #     workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
    #     with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
    #         rp.write(rocksdb_properties)
    #     with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
    #         wl.write(workload_conf)
    #     conf_id_list.append((conf_id, threads))
    #     log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
    #     # command_list.append(LOAD_CMD.format(threads=threads, conf_id=conf_id, log_path=log_path))
    #     # command_list.append(RUN_WITH_INIT_CMD.format(threads=threads, conf_id=conf_id, log_path=log_path))
    #     command_list.append(RUN_WITHOUT_INIT_CMD.format(threads=threads, conf_id=conf_id, log_path=log_path))
    for version in ['debug-v0']:
        # for workload in ['b']:
        #     for operationcount in [20000000]:
        #         # for write_buffer_size in (np.array([1])*1024**3).tolist():
        #         # for write_buffer_size in (np.array([0.0625,0.125,0.25,0.5,1,2,4,8,16,32,48])*1024**3).tolist():
        #         for write_buffer_size in (np.array([1,4,16,32,64])*1024**3).tolist():
        #             cache_size = 0
        #             recordcount = 134217728
        #             threads = 16    
        #             min_write_buffer_number_to_merge = max_write_buffer_number - 2
        #             conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution,max_write_buffer_number=max_write_buffer_number,max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size,max_write_buffer_number=max_write_buffer_number, dbname=DBNAMES[workload],max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
        #             with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
        #                 rp.write(rocksdb_properties)
        #             with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
        #                 wl.write(workload_conf)
        #             conf_id_list.append((conf_id, threads))
        #             log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
        #             command_list.append(RUN_CMDS[workload].format(threads=threads, conf_id=conf_id, log_path=log_path))
        # for workload in ['b']:
        #     for operationcount in [20000000]:
        #         for cache_size in (np.array([1])*1024**3).tolist():
        #             min_write_buffer_number_to_merge = max_write_buffer_number - 2
        #             write_buffer_size = 1*1024**3
        #             recordcount = 134217728
        #             threads = 1
        #             conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution,max_write_buffer_number=max_write_buffer_number,max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size,max_write_buffer_number=max_write_buffer_number, dbname=DBNAMES[workload],max_background_jobs=max_background_jobs,table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
        #             with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
        #                 rp.write(rocksdb_properties)
        #             with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
        #                 wl.write(workload_conf)
        #             conf_id_list.append((conf_id, threads))
        #             log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
        #             command_list.append(RUN_CMDS[workload].format(threads=threads, conf_id=conf_id, log_path=log_path))
        
        workload = 'c'
        operationcount = 20000000
        cache_size = int(32*1024**3)
        # cache_size = 256*1024**2
        # rm_ratio = 0.0
        min_write_buffer_number_to_merge = max_write_buffer_number - 2
        write_buffer_size = 256*1024**2
        recordcount = 134217728
        threads = 8
        table_cache_numshardbits = 4
        for rm_ratio in [0.0, 0.1, 0.5, 0.8]:
            conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution,max_write_buffer_number=max_write_buffer_number,max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge, rm_ratio=rm_ratio)
            rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size,max_write_buffer_number=max_write_buffer_number, dbname=DBNAMES[workload],max_background_jobs=max_background_jobs,table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge, rm_ratio=rm_ratio)
            workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
            with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
                rp.write(rocksdb_properties)
            with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
                wl.write(workload_conf)
            conf_id_list.append((conf_id, threads))
            log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
            command_list.append(RUN_CMDS[workload].format(threads=threads, conf_id=conf_id, log_path=log_path))
        # for workload in ['b']:
        #     for operationcount in [20000000]:
        #         write_buffer_size =1*1024**3
        #         for max_write_buffer_number in [4,16,32,64,128]:
        #             min_write_buffer_number_to_merge = max_write_buffer_number - 2
        #             cache_size = 0
        #             recordcount = 134217728
        #             threads = 32
        #             conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution,max_write_buffer_number=max_write_buffer_number,max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size,max_write_buffer_number=max_write_buffer_number, dbname=DBNAMES[workload],max_background_jobs=max_background_jobs, table_cache_numshardbits=table_cache_numshardbits,min_write_buffer_number_to_merge=min_write_buffer_number_to_merge)
        #             workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
        #             with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
        #                 rp.write(rocksdb_properties)
        #             with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
        #                 wl.write(workload_conf)
        #             conf_id_list.append((conf_id, threads))
        #             log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
        #             command_list.append(RUN_CMDS[workload].format(threads=threads, conf_id=conf_id, log_path=log_path))
        ### xyy
        # workload = 'b'
        # operationcount = 20000000
        # cache_size = 30 * 1024**3
        # write_buffer_size = 1 * 1024**3
        # recordcount = 100000000
        # threads = 79
        # max_write_buffer_number = 4
        # max_background_jobs = 6
        # conf_id = mngr.get_id(workload=workload, recordcount=recordcount, operationcount=operationcount, threads=threads, write_buffer_size=write_buffer_size, cache_size=cache_size, version=version, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution,max_write_buffer_number=max_write_buffer_number,max_background_jobs=max_background_jobs)
        # rocksdb_properties = ROCKSDB_PROPERTIES.format(workload=workload, write_buffer_size=write_buffer_size, cache_size=cache_size,max_write_buffer_number=max_write_buffer_number, dbname=DBNAMES[workload],max_background_jobs=max_background_jobs)
        # workload_conf = WORKLOADS[workload].format(recordcount=recordcount, operationcount=operationcount, zipfian_alpha=zipfian_alpha, requestdistribution=requestdistribution)
        # with open(osp.join(DB_DIRNAME, f'rocksdb_properties/{conf_id}.properties'), 'w') as rp:
        #     rp.write(rocksdb_properties)
        # with open(osp.join(DB_DIRNAME, f'workload/{conf_id}'), 'w') as wl:
        #     wl.write(workload_conf)
        # conf_id_list.append((conf_id, threads))
        # log_path = osp.join(mngr.remote_logs_dirname, f"{conf_id}.log")
        # command_list.append(RUN_CMDS[workload].format(threads=threads, conf_id=conf_id, log_path=log_path))
    # for command in sorted(list(set(command_list))):
    for command in command_list:
        # if osp.exists(command.split(' ')[-1]):
        #     continue
        print(command)

