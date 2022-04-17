ps -ef | grep ycsb | grep -v tee | grep -v grep | awk '{print $2}'  

/sys/devices/system/cpu/smt/control

## Page Cache

cat /proc/`ps -ef | grep ycsb | grep -v tee | grep -v grep | awk '{print $2}'`/status

## Perf

sudo perf record -F 500 -g -p `ps -ef | grep ycsb | grep -v tee | grep -v grep | awk '{print $2}'` && sudo chmod 666 perf.data

time perf script -i perf.data > perf.unfold
time ../FlameGraph/stackcollapse-perf.pl perf.unfold > perf.folded
time ../FlameGraph/flamegraph.pl perf.folded > perf.svg

./db_bench --db=/home/wzh/nvme/tmp_db --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 --value_size=1000 --cache_size=17179869184 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 --min_level_to_compress=-1 --disable_seek_compaction=1 --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --disable_wal=0 --wal_dir=/home/wzh/nvme/tmp_db/WAL_LOG --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=1048576 --histogram=0 --use_plain_table=0 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash --bloom_bits=10 --bloom_locality=1 --benchmarks=filluniquerandom --use_existing_db=0 --num=100000000 --threads=1 --allow_concurrent_memtable_write=false

