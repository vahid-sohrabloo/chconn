package setting

import (
	"io"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Settings for clickhouse query setting
// for more information read
// https://clickhouse.tech/docs/en/operations/settings/settings/
// Note: all settings auto generatted and some setting may not affected.
// Because we use native TCP connections and some setting only use for HTTP connections
type Settings struct {
	configs map[string]interface{}
	dirty   bool
	w       *readerwriter.Writer
}

// NewSettings return new settings for clickhouse query setting
// for more information read
// https://clickhouse.tech/docs/en/operations/settings/settings/
func NewSettings() *Settings {
	return &Settings{
		configs: make(map[string]interface{}),
		w:       readerwriter.NewWriter(),
	}
}

// WriteTo write settings to writer
//
// it uses internally
func (s *Settings) WriteTo(wt io.Writer, asString bool) (int, error) {
	// todo handle asString for old protocols
	if s.dirty {
		s.w.Reset()
		s.dirty = false
		for key, v := range s.configs {
			s.w.String(key)
			// flag
			s.w.Uint8(0)
			switch val := v.(type) {
			case uint64:
				s.w.String(strconv.FormatUint(val, 10))
			case int64:
				s.w.String(strconv.FormatInt(val, 10))
			case string:
				s.w.String(val)
			case bool:
				if val {
					s.w.String("1")
				} else {
					s.w.String("0")
				}

			case byte:
				s.w.String(string(val))
			default:
				panic("not support type")
			}
		}
	}
	return wt.Write(s.w.Output().Bytes())
}

// MinCompressBlockSize set min_compress_block_size setting
// The actual size of the block to compress, if the uncompressed data less than
// max_compress_block_size is no less than this value and no less than the volume
// of data for one mark.
func (s *Settings) MinCompressBlockSize(v uint64) {
	s.configs["min_compress_block_size"] = v
	s.dirty = true
}

// MaxCompressBlockSize set max_compress_block_size setting
// The maximum size of blocks of uncompressed data before compressing for writing
// to a table.
func (s *Settings) MaxCompressBlockSize(v uint64) {
	s.configs["max_compress_block_size"] = v
	s.dirty = true
}

// MaxBlockSize set max_block_size setting
// Maximum block size for reading
func (s *Settings) MaxBlockSize(v uint64) {
	s.configs["max_block_size"] = v
	s.dirty = true
}

// MaxInsertBlockSize set max_insert_block_size setting
// The maximum block size for insertion, if we control the creation of blocks for
// insertion.
func (s *Settings) MaxInsertBlockSize(v uint64) {
	s.configs["max_insert_block_size"] = v
	s.dirty = true
}

// MinInsertBlockSizeRows set min_insert_block_size_rows setting
// Squash blocks passed to INSERT query to specified size in rows, if blocks are
// not big enough.
func (s *Settings) MinInsertBlockSizeRows(v uint64) {
	s.configs["min_insert_block_size_rows"] = v
	s.dirty = true
}

// MinInsertBlockSizeRowsForMaterializedViews set min_insert_block_size_rows_for_materialized_views setting
// Like min_insert_block_size_rows, but applied only during pushing to MATERIALIZED
// VIEW (default: min_insert_block_size_rows)
func (s *Settings) MinInsertBlockSizeRowsForMaterializedViews(v uint64) {
	s.configs["min_insert_block_size_rows_for_materialized_views"] = v
	s.dirty = true
}

// MinInsertBlockSizeBytesForMaterializedViews set min_insert_block_size_bytes_for_materialized_views setting
// Like min_insert_block_size_bytes, but applied only during pushing to
// MATERIALIZED VIEW (default: min_insert_block_size_bytes)
func (s *Settings) MinInsertBlockSizeBytesForMaterializedViews(v uint64) {
	s.configs["min_insert_block_size_bytes_for_materialized_views"] = v
	s.dirty = true
}

// MaxJoinedBlockSizeRows set max_joined_block_size_rows setting
// Maximum block size for JOIN result (if join algorithm supports it). 0 means
// unlimited.
func (s *Settings) MaxJoinedBlockSizeRows(v uint64) {
	s.configs["max_joined_block_size_rows"] = v
	s.dirty = true
}

// MaxInsertThreads set max_insert_threads setting
// The maximum number of threads to execute the INSERT SELECT query. Values 0 or 1
// means that INSERT SELECT is not run in parallel. Higher values will lead to
// higher memory usage. Parallel INSERT SELECT has effect only if the SELECT part
// is run on parallel, see 'max_threads' setting.
func (s *Settings) MaxInsertThreads(v uint64) {
	s.configs["max_insert_threads"] = v
	s.dirty = true
}

// MaxFinalThreads set max_final_threads setting
// The maximum number of threads to read from table with FINAL.
func (s *Settings) MaxFinalThreads(v uint64) {
	s.configs["max_final_threads"] = v
	s.dirty = true
}

// MaxThreads set max_threads setting
// The maximum number of threads to execute the request. By default, it is
// determined automatically.
func (s *Settings) MaxThreads(v uint64) {
	s.configs["max_threads"] = v
	s.dirty = true
}

// MaxAlterThreads set max_alter_threads setting
// The maximum number of threads to execute the ALTER requests. By default, it is
// determined automatically.
func (s *Settings) MaxAlterThreads(v uint64) {
	s.configs["max_alter_threads"] = v
	s.dirty = true
}

// MaxReadBufferSize set max_read_buffer_size setting
// The maximum size of the buffer to read from the filesystem.
func (s *Settings) MaxReadBufferSize(v uint64) {
	s.configs["max_read_buffer_size"] = v
	s.dirty = true
}

// MaxDistributedConnections set max_distributed_connections setting
// The maximum number of connections for distributed processing of one query
// (should be greater than max_threads).
func (s *Settings) MaxDistributedConnections(v uint64) {
	s.configs["max_distributed_connections"] = v
	s.dirty = true
}

// MaxQuerySize set max_query_size setting
// Which part of the query can be read into RAM for parsing (the remaining data for
// INSERT, if any, is read later)
func (s *Settings) MaxQuerySize(v uint64) {
	s.configs["max_query_size"] = v
	s.dirty = true
}

// InteractiveDelay set interactive_delay setting
// The interval in microseconds to check if the request is canceled, and to send
// progress info.
func (s *Settings) InteractiveDelay(v uint64) {
	s.configs["interactive_delay"] = v
	s.dirty = true
}

// ConnectTimeout set connect_timeout setting
// Connection timeout if there are no replicas.
func (s *Settings) ConnectTimeout(v time.Duration) {
	s.configs["connect_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// ConnectTimeoutWithFailoverMs set connect_timeout_with_failover_ms setting
// Connection timeout for selecting first healthy replica.
func (s *Settings) ConnectTimeoutWithFailoverMs(v time.Duration) {
	s.configs["connect_timeout_with_failover_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ConnectTimeoutWithFailoverSecureMs set connect_timeout_with_failover_secure_ms setting
// Connection timeout for selecting first healthy replica (for secure connections).
func (s *Settings) ConnectTimeoutWithFailoverSecureMs(v time.Duration) {
	s.configs["connect_timeout_with_failover_secure_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ReceiveTimeout set receive_timeout setting
//
func (s *Settings) ReceiveTimeout(v time.Duration) {
	s.configs["receive_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// SendTimeout set send_timeout setting
//
func (s *Settings) SendTimeout(v time.Duration) {
	s.configs["send_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// TCPKeepAliveTimeout set tcp_keep_alive_timeout setting
// The time in seconds the connection needs to remain idle before TCP starts
// sending keepalive probes
func (s *Settings) TCPKeepAliveTimeout(v time.Duration) {
	s.configs["tcp_keep_alive_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// QueueMaxWaitMs set queue_max_wait_ms setting
// The wait time in the request queue, if the number of concurrent requests exceeds
// the maximum.
func (s *Settings) QueueMaxWaitMs(v time.Duration) {
	s.configs["queue_max_wait_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ConnectionPoolMaxWaitMs set connection_pool_max_wait_ms setting
// The wait time when the connection pool is full.
func (s *Settings) ConnectionPoolMaxWaitMs(v time.Duration) {
	s.configs["connection_pool_max_wait_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ReplaceRunningQueryMaxWaitMs set replace_running_query_max_wait_ms setting
// The wait time for running query with the same query_id to finish when setting
// 'replace_running_query' is active.
func (s *Settings) ReplaceRunningQueryMaxWaitMs(v time.Duration) {
	s.configs["replace_running_query_max_wait_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// KafkaMaxWaitMs set kafka_max_wait_ms setting
// The wait time for reading from Kafka before retry.
func (s *Settings) KafkaMaxWaitMs(v time.Duration) {
	s.configs["kafka_max_wait_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// RabbitmqMaxWaitMs set rabbitmq_max_wait_ms setting
// The wait time for reading from RabbitMQ before retry.
func (s *Settings) RabbitmqMaxWaitMs(v time.Duration) {
	s.configs["rabbitmq_max_wait_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// PollInterval set poll_interval setting
// Block at the query wait loop on the server for the specified number of seconds.
func (s *Settings) PollInterval(v uint64) {
	s.configs["poll_interval"] = v
	s.dirty = true
}

// IdleConnectionTimeout set idle_connection_timeout setting
// Close idle TCP connections after specified number of seconds.
func (s *Settings) IdleConnectionTimeout(v uint64) {
	s.configs["idle_connection_timeout"] = v
	s.dirty = true
}

// DistributedConnectionsPoolSize set distributed_connections_pool_size setting
// Maximum number of connections with one remote server in the pool.
func (s *Settings) DistributedConnectionsPoolSize(v uint64) {
	s.configs["distributed_connections_pool_size"] = v
	s.dirty = true
}

// ConnectionsWithFailoverMaxTries set connections_with_failover_max_tries setting
// The maximum number of attempts to connect to replicas.
func (s *Settings) ConnectionsWithFailoverMaxTries(v uint64) {
	s.configs["connections_with_failover_max_tries"] = v
	s.dirty = true
}

// Extremes set extremes setting
// Calculate minimums and maximums of the result columns. They can be output in
// JSON-formats.
func (s *Settings) Extremes(v bool) {
	s.configs["extremes"] = v
	s.dirty = true
}

// UseUncompressedCache set use_uncompressed_cache setting
// Whether to use the cache of uncompressed blocks.
func (s *Settings) UseUncompressedCache(v bool) {
	s.configs["use_uncompressed_cache"] = v
	s.dirty = true
}

// ReplaceRunningQuery set replace_running_query setting
// Whether the running request should be canceled with the same id as the new one.
func (s *Settings) ReplaceRunningQuery(v bool) {
	s.configs["replace_running_query"] = v
	s.dirty = true
}

// BackgroundBufferFlushSchedulePoolSize set background_buffer_flush_schedule_pool_size setting
// Number of threads performing background flush for tables with Buffer engine.
// Only has meaning at server startup.
func (s *Settings) BackgroundBufferFlushSchedulePoolSize(v uint64) {
	s.configs["background_buffer_flush_schedule_pool_size"] = v
	s.dirty = true
}

// BackgroundPoolSize set background_pool_size setting
// Number of threads performing background work for tables (for example, merging in
// merge tree). Only has meaning at server startup.
func (s *Settings) BackgroundPoolSize(v uint64) {
	s.configs["background_pool_size"] = v
	s.dirty = true
}

// BackgroundMovePoolSize set background_move_pool_size setting
// Number of threads performing background moves for tables. Only has meaning at
// server startup.
func (s *Settings) BackgroundMovePoolSize(v uint64) {
	s.configs["background_move_pool_size"] = v
	s.dirty = true
}

// BackgroundSchedulePoolSize set background_schedule_pool_size setting
// Number of threads performing background tasks for replicated tables, kafka
// streaming, dns cache updates. Only has meaning at server startup.
func (s *Settings) BackgroundSchedulePoolSize(v uint64) {
	s.configs["background_schedule_pool_size"] = v
	s.dirty = true
}

// BackgroundDistributedSchedulePoolSize set background_distributed_schedule_pool_size setting
// Number of threads performing background tasks for distributed sends. Only has
// meaning at server startup.
func (s *Settings) BackgroundDistributedSchedulePoolSize(v uint64) {
	s.configs["background_distributed_schedule_pool_size"] = v
	s.dirty = true
}

// DistributedDirectoryMonitorSleepTimeMs set distributed_directory_monitor_sleep_time_ms setting
// Sleep time for StorageDistributed DirectoryMonitors, in case of any errors delay
// grows exponentially.
func (s *Settings) DistributedDirectoryMonitorSleepTimeMs(v time.Duration) {
	s.configs["distributed_directory_monitor_sleep_time_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// DistributedDirectoryMonitorMaxSleepTimeMs set distributed_directory_monitor_max_sleep_time_ms setting
// Maximum sleep time for StorageDistributed DirectoryMonitors, it limits
// exponential growth too.
func (s *Settings) DistributedDirectoryMonitorMaxSleepTimeMs(v time.Duration) {
	s.configs["distributed_directory_monitor_max_sleep_time_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// DistributedDirectoryMonitorBatchInserts set distributed_directory_monitor_batch_inserts setting
// Should StorageDistributed DirectoryMonitors try to batch individual inserts into
// bigger ones.
func (s *Settings) DistributedDirectoryMonitorBatchInserts(v bool) {
	s.configs["distributed_directory_monitor_batch_inserts"] = v
	s.dirty = true
}

// OptimizeMoveToPrewhere set optimize_move_to_prewhere setting
// Allows disabling WHERE to PREWHERE optimization in SELECT queries from
// MergeTree.
func (s *Settings) OptimizeMoveToPrewhere(v bool) {
	s.configs["optimize_move_to_prewhere"] = v
	s.dirty = true
}

// ReplicationAlterPartitionsSync set replication_alter_partitions_sync setting
// Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for
// execution only of itself, 2 - wait for everyone.
func (s *Settings) ReplicationAlterPartitionsSync(v uint64) {
	s.configs["replication_alter_partitions_sync"] = v
	s.dirty = true
}

// ReplicationAlterColumnsTimeout set replication_alter_columns_timeout setting
// Wait for actions to change the table structure within the specified number of
// seconds. 0 - wait unlimited time.
func (s *Settings) ReplicationAlterColumnsTimeout(v uint64) {
	s.configs["replication_alter_columns_timeout"] = v
	s.dirty = true
}

// AllowSuspiciousLowCardinalityTypes set allow_suspicious_low_cardinality_types setting
// In CREATE TABLE statement allows specifying LowCardinality modifier for types of
// small fixed size (8 or less). Enabling this may increase merge times and memory
// consumption.
func (s *Settings) AllowSuspiciousLowCardinalityTypes(v bool) {
	s.configs["allow_suspicious_low_cardinality_types"] = v
	s.dirty = true
}

// CompileExpressions set compile_expressions setting
// Compile some scalar functions and operators to native code.
func (s *Settings) CompileExpressions(v bool) {
	s.configs["compile_expressions"] = v
	s.dirty = true
}

// MinCountToCompileExpression set min_count_to_compile_expression setting
// The number of identical expressions before they are JIT-compiled
func (s *Settings) MinCountToCompileExpression(v uint64) {
	s.configs["min_count_to_compile_expression"] = v
	s.dirty = true
}

// GroupByTwoLevelThreshold set group_by_two_level_threshold setting
// From what number of keys, a two-level aggregation starts. 0 - the threshold is
// not set.
func (s *Settings) GroupByTwoLevelThreshold(v uint64) {
	s.configs["group_by_two_level_threshold"] = v
	s.dirty = true
}

// GroupByTwoLevelThresholdBytes set group_by_two_level_threshold_bytes setting
// From what size of the aggregation state in bytes, a two-level aggregation begins
// to be used. 0 - the threshold is not set. Two-level aggregation is used when at
// least one of the thresholds is triggered.
func (s *Settings) GroupByTwoLevelThresholdBytes(v uint64) {
	s.configs["group_by_two_level_threshold_bytes"] = v
	s.dirty = true
}

// DistributedAggregationMemoryEfficient set distributed_aggregation_memory_efficient setting
// Is the memory-saving mode of distributed aggregation enabled.
func (s *Settings) DistributedAggregationMemoryEfficient(v bool) {
	s.configs["distributed_aggregation_memory_efficient"] = v
	s.dirty = true
}

// AggregationMemoryEfficientMergeThreads set aggregation_memory_efficient_merge_threads setting
// Number of threads to use for merge intermediate aggregation results in memory
// efficient mode. When bigger, then more memory is consumed. 0 means - same as
// 'max_threads'.
func (s *Settings) AggregationMemoryEfficientMergeThreads(v uint64) {
	s.configs["aggregation_memory_efficient_merge_threads"] = v
	s.dirty = true
}

// MaxParallelReplicas set max_parallel_replicas setting
// The maximum number of replicas of each shard used when the query is executed.
// For consistency (to get different parts of the same partition), this option only
// works for the specified sampling key. The lag of the replicas is not controlled.
func (s *Settings) MaxParallelReplicas(v uint64) {
	s.configs["max_parallel_replicas"] = v
	s.dirty = true
}

// ParallelReplicasCount set parallel_replicas_count setting
//
func (s *Settings) ParallelReplicasCount(v uint64) {
	s.configs["parallel_replicas_count"] = v
	s.dirty = true
}

// ParallelReplicaOffset set parallel_replica_offset setting
//
func (s *Settings) ParallelReplicaOffset(v uint64) {
	s.configs["parallel_replica_offset"] = v
	s.dirty = true
}

// SkipUnavailableShards set skip_unavailable_shards setting
// If 1, ClickHouse silently skips unavailable shards and nodes unresolvable
// through DNS. Shard is marked as unavailable when none of the replicas can be
// reached.
func (s *Settings) SkipUnavailableShards(v bool) {
	s.configs["skip_unavailable_shards"] = v
	s.dirty = true
}

// DistributedGroupByNoMerge set distributed_group_by_no_merge setting
// Do not merge aggregation states from different servers for distributed query
// processing - in case it is for certain that there are different keys on
// different shards.
func (s *Settings) DistributedGroupByNoMerge(v bool) {
	s.configs["distributed_group_by_no_merge"] = v
	s.dirty = true
}

// ParallelDistributedInsertSelect set parallel_distributed_insert_select setting
// If true, distributed insert select query in the same cluster will be processed
// on local tables on every shard
func (s *Settings) ParallelDistributedInsertSelect(v bool) {
	s.configs["parallel_distributed_insert_select"] = v
	s.dirty = true
}

// OptimizeDistributedGroupByShardingKey set optimize_distributed_group_by_sharding_key setting
// Optimize GROUP BY sharding_key queries (by avodiing costly aggregation on the
// initiator server).
func (s *Settings) OptimizeDistributedGroupByShardingKey(v bool) {
	s.configs["optimize_distributed_group_by_sharding_key"] = v
	s.dirty = true
}

// OptimizeSkipUnusedShards set optimize_skip_unused_shards setting
// Assumes that data is distributed by sharding_key. Optimization to skip unused
// shards if SELECT query filters by sharding_key.
func (s *Settings) OptimizeSkipUnusedShards(v bool) {
	s.configs["optimize_skip_unused_shards"] = v
	s.dirty = true
}

// ForceOptimizeSkipUnusedShards set force_optimize_skip_unused_shards setting
// Throw an exception if unused shards cannot be skipped (1 - throw only if the
// table has the sharding key, 2 - always throw.
func (s *Settings) ForceOptimizeSkipUnusedShards(v uint64) {
	s.configs["force_optimize_skip_unused_shards"] = v
	s.dirty = true
}

// OptimizeSkipUnusedShardsNesting set optimize_skip_unused_shards_nesting setting
// Same as optimize_skip_unused_shards, but accept nesting level until which it
// will work.
func (s *Settings) OptimizeSkipUnusedShardsNesting(v uint64) {
	s.configs["optimize_skip_unused_shards_nesting"] = v
	s.dirty = true
}

// ForceOptimizeSkipUnusedShardsNesting set force_optimize_skip_unused_shards_nesting setting
// Same as force_optimize_skip_unused_shards, but accept nesting level until which
// it will work.
func (s *Settings) ForceOptimizeSkipUnusedShardsNesting(v uint64) {
	s.configs["force_optimize_skip_unused_shards_nesting"] = v
	s.dirty = true
}

// InputFormatParallelParsing set input_format_parallel_parsing setting
// Enable parallel parsing for some data formats.
func (s *Settings) InputFormatParallelParsing(v bool) {
	s.configs["input_format_parallel_parsing"] = v
	s.dirty = true
}

// MergeTreeMinRowsForSeek set merge_tree_min_rows_for_seek setting
// You can skip reading more than that number of rows at the price of one seek per
// file.
func (s *Settings) MergeTreeMinRowsForSeek(v uint64) {
	s.configs["merge_tree_min_rows_for_seek"] = v
	s.dirty = true
}

// MergeTreeMinBytesForSeek set merge_tree_min_bytes_for_seek setting
// You can skip reading more than that number of bytes at the price of one seek per
// file.
func (s *Settings) MergeTreeMinBytesForSeek(v uint64) {
	s.configs["merge_tree_min_bytes_for_seek"] = v
	s.dirty = true
}

// MergeTreeCoarseIndexGranularity set merge_tree_coarse_index_granularity setting
// If the index segment can contain the required keys, divide it into as many parts
// and recursively check them.
func (s *Settings) MergeTreeCoarseIndexGranularity(v uint64) {
	s.configs["merge_tree_coarse_index_granularity"] = v
	s.dirty = true
}

// MysqlMaxRowsToInsert set mysql_max_rows_to_insert setting
// The maximum number of rows in MySQL batch insertion of the MySQL storage engine
func (s *Settings) MysqlMaxRowsToInsert(v uint64) {
	s.configs["mysql_max_rows_to_insert"] = v
	s.dirty = true
}

// OptimizeMinEqualityDisjunctionChainLength set optimize_min_equality_disjunction_chain_length setting
// The minimum length of the expression "expr = x1 OR ... expr = xN" for
// optimization
func (s *Settings) OptimizeMinEqualityDisjunctionChainLength(v uint64) {
	s.configs["optimize_min_equality_disjunction_chain_length"] = v
	s.dirty = true
}

// MinBytesToUseDirectIo set min_bytes_to_use_direct_io setting
// The minimum number of bytes for reading the data with O_DIRECT option during
// SELECT queries execution. 0 - disabled.
func (s *Settings) MinBytesToUseDirectIo(v uint64) {
	s.configs["min_bytes_to_use_direct_io"] = v
	s.dirty = true
}

// MinBytesToUseMmapIo set min_bytes_to_use_mmap_io setting
// The minimum number of bytes for reading the data with mmap option during SELECT
// queries execution. 0 - disabled.
func (s *Settings) MinBytesToUseMmapIo(v uint64) {
	s.configs["min_bytes_to_use_mmap_io"] = v
	s.dirty = true
}

// ForceIndexByDate set force_index_by_date setting
// Throw an exception if there is a partition key in a table, and it is not used.
func (s *Settings) ForceIndexByDate(v bool) {
	s.configs["force_index_by_date"] = v
	s.dirty = true
}

// ForcePrimaryKey set force_primary_key setting
// Throw an exception if there is primary key in a table, and it is not used.
func (s *Settings) ForcePrimaryKey(v bool) {
	s.configs["force_primary_key"] = v
	s.dirty = true
}

// MaxStreamsToMaxThreadsRatio set max_streams_to_max_threads_ratio setting
// Allows you to use more sources than the number of threads - to more evenly
// distribute work across threads. It is assumed that this is a temporary solution,
// since it will be possible in the future to make the number of sources equal to
// the number of threads, but for each source to dynamically select available work
// for itself.
func (s *Settings) MaxStreamsToMaxThreadsRatio(v string) {
	s.configs["max_streams_to_max_threads_ratio"] = v
	s.dirty = true
}

// MaxStreamsMultiplierForMergeTables set max_streams_multiplier_for_merge_tables setting
// Ask more streams when reading from Merge table. Streams will be spread across
// tables that Merge table will use. This allows more even distribution of work
// across threads and especially helpful when merged tables differ in size.
func (s *Settings) MaxStreamsMultiplierForMergeTables(v string) {
	s.configs["max_streams_multiplier_for_merge_tables"] = v
	s.dirty = true
}

// NetworkZstdCompressionLevel set network_zstd_compression_level setting
// Allows you to select the level of ZSTD compression.
func (s *Settings) NetworkZstdCompressionLevel(v int64) {
	s.configs["network_zstd_compression_level"] = v
	s.dirty = true
}

// Priority set priority setting
// Priority of the query. 1 - the highest, higher value - lower priority; 0 - do
// not use priorities.
func (s *Settings) Priority(v uint64) {
	s.configs["priority"] = v
	s.dirty = true
}

// OsThreadPriority set os_thread_priority setting
// If non zero - set corresponding 'nice' value for query processing threads. Can
// be used to adjust query priority for OS scheduler.
func (s *Settings) OsThreadPriority(v int64) {
	s.configs["os_thread_priority"] = v
	s.dirty = true
}

// LogQueries set log_queries setting
// Log requests and write the log to the system table.
func (s *Settings) LogQueries(v bool) {
	s.configs["log_queries"] = v
	s.dirty = true
}

// LogQueriesCutToLength set log_queries_cut_to_length setting
// If query length is greater than specified threshold (in bytes), then cut query
// when writing to query log. Also limit length of printed query in ordinary text
// log.
func (s *Settings) LogQueriesCutToLength(v uint64) {
	s.configs["log_queries_cut_to_length"] = v
	s.dirty = true
}

// MaxConcurrentQueriesForUser set max_concurrent_queries_for_user setting
// The maximum number of concurrent requests per user.
func (s *Settings) MaxConcurrentQueriesForUser(v uint64) {
	s.configs["max_concurrent_queries_for_user"] = v
	s.dirty = true
}

// InsertDeduplicate set insert_deduplicate setting
// For INSERT queries in the replicated table, specifies that deduplication of
// insertings blocks should be preformed
func (s *Settings) InsertDeduplicate(v bool) {
	s.configs["insert_deduplicate"] = v
	s.dirty = true
}

// InsertQuorum set insert_quorum setting
// For INSERT queries in the replicated table, wait writing for the specified
// number of replicas and linearize the addition of the data. 0 - disabled.
func (s *Settings) InsertQuorum(v uint64) {
	s.configs["insert_quorum"] = v
	s.dirty = true
}

// InsertQuorumTimeout set insert_quorum_timeout setting
//
func (s *Settings) InsertQuorumTimeout(v time.Duration) {
	s.configs["insert_quorum_timeout"] = uint64(v.Milliseconds())
	s.dirty = true
}

// SelectSequentialConsistency set select_sequential_consistency setting
// For SELECT queries from the replicated table, throw an exception if the replica
// does not have a chunk written with the quorum; do not read the parts that have
// not yet been written with the quorum.
func (s *Settings) SelectSequentialConsistency(v uint64) {
	s.configs["select_sequential_consistency"] = v
	s.dirty = true
}

// TableFunctionRemoteMaxAddresses set table_function_remote_max_addresses setting
// The maximum number of different shards and the maximum number of replicas of one
// shard in the "remote" function.
func (s *Settings) TableFunctionRemoteMaxAddresses(v uint64) {
	s.configs["table_function_remote_max_addresses"] = v
	s.dirty = true
}

// ReadBackoffMinLatencyMs set read_backoff_min_latency_ms setting
// Setting to reduce the number of threads in case of slow reads. Pay attention
// only to reads that took at least that much time.
func (s *Settings) ReadBackoffMinLatencyMs(v time.Duration) {
	s.configs["read_backoff_min_latency_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ReadBackoffMaxThroughput set read_backoff_max_throughput setting
// Settings to reduce the number of threads in case of slow reads. Count events
// when the read bandwidth is less than that many bytes per second.
func (s *Settings) ReadBackoffMaxThroughput(v uint64) {
	s.configs["read_backoff_max_throughput"] = v
	s.dirty = true
}

// ReadBackoffMinIntervalBetweenEventsMs set read_backoff_min_interval_between_events_ms setting
// Settings to reduce the number of threads in case of slow reads. Do not pay
// attention to the event, if the previous one has passed less than a certain
// amount of time.
func (s *Settings) ReadBackoffMinIntervalBetweenEventsMs(v time.Duration) {
	s.configs["read_backoff_min_interval_between_events_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// ReadBackoffMinEvents set read_backoff_min_events setting
// Settings to reduce the number of threads in case of slow reads. The number of
// events after which the number of threads will be reduced.
func (s *Settings) ReadBackoffMinEvents(v uint64) {
	s.configs["read_backoff_min_events"] = v
	s.dirty = true
}

// UseClientTimeZone set use_client_time_zone setting
// Use client timezone for interpreting DateTime string values, instead of adopting
// server timezone.
func (s *Settings) UseClientTimeZone(v bool) {
	s.configs["use_client_time_zone"] = v
	s.dirty = true
}

// FsyncMetadata set fsync_metadata setting
// Do fsync after changing metadata for tables and databases (.sql files). Could be
// disabled in case of poor latency on server with high load of DDL queries and
// high load of disk subsystem.
func (s *Settings) FsyncMetadata(v bool) {
	s.configs["fsync_metadata"] = v
	s.dirty = true
}

// JoinUseNulls set join_use_nulls setting
// Use NULLs for non-joined rows of outer JOINs for types that can be inside
// Nullable. If false, use default value of corresponding columns data type.
func (s *Settings) JoinUseNulls(v bool) {
	s.configs["join_use_nulls"] = v
	s.dirty = true
}

// AnyJoinDistinctRightTableKeys set any_join_distinct_right_table_keys setting
// Enable old ANY JOIN logic with many-to-one left-to-right table keys mapping for
// all ANY JOINs. It leads to confusing not equal results for 't1 ANY LEFT JOIN t2'
// and 't2 ANY RIGHT JOIN t1'. ANY RIGHT JOIN needs one-to-many keys mapping to be
// consistent with LEFT one.
func (s *Settings) AnyJoinDistinctRightTableKeys(v bool) {
	s.configs["any_join_distinct_right_table_keys"] = v
	s.dirty = true
}

// PreferredBlockSizeBytes set preferred_block_size_bytes setting
//
func (s *Settings) PreferredBlockSizeBytes(v uint64) {
	s.configs["preferred_block_size_bytes"] = v
	s.dirty = true
}

// MaxReplicaDelayForDistributedQueries set max_replica_delay_for_distributed_queries setting
// If set, distributed queries of Replicated tables will choose servers with
// replication delay in seconds less than the specified value (not inclusive). Zero
// means do not take delay into account.
func (s *Settings) MaxReplicaDelayForDistributedQueries(v uint64) {
	s.configs["max_replica_delay_for_distributed_queries"] = v
	s.dirty = true
}

// FallbackToStaleReplicasForDistributedQueries set fallback_to_stale_replicas_for_distributed_queries setting
// Suppose max_replica_delay_for_distributed_queries is set and all replicas for
// the queried table are stale. If this setting is enabled, the query will be
// performed anyway, otherwise the error will be reported.
func (s *Settings) FallbackToStaleReplicasForDistributedQueries(v bool) {
	s.configs["fallback_to_stale_replicas_for_distributed_queries"] = v
	s.dirty = true
}

// PreferredMaxColumnInBlockSizeBytes set preferred_max_column_in_block_size_bytes setting
// Limit on max column size in block while reading. Helps to decrease cache misses
// count. Should be close to L2 cache size.
func (s *Settings) PreferredMaxColumnInBlockSizeBytes(v uint64) {
	s.configs["preferred_max_column_in_block_size_bytes"] = v
	s.dirty = true
}

// InsertDistributedSync set insert_distributed_sync setting
// If setting is enabled, insert query into distributed waits until data will be
// sent to all nodes in cluster.
func (s *Settings) InsertDistributedSync(v bool) {
	s.configs["insert_distributed_sync"] = v
	s.dirty = true
}

// InsertDistributedTimeout set insert_distributed_timeout setting
// Timeout for insert query into distributed. Setting is used only with
// insert_distributed_sync enabled. Zero value means no timeout.
func (s *Settings) InsertDistributedTimeout(v uint64) {
	s.configs["insert_distributed_timeout"] = v
	s.dirty = true
}

// DistributedDDLTaskTimeout set distributed_ddl_task_timeout setting
// Timeout for DDL query responses from all hosts in cluster. If a ddl request has
// not been performed on all hosts, a response will contain a timeout error and a
// request will be executed in an async mode. Negative value means infinite.
func (s *Settings) DistributedDDLTaskTimeout(v int64) {
	s.configs["distributed_ddl_task_timeout"] = v
	s.dirty = true
}

// StreamFlushIntervalMs set stream_flush_interval_ms setting
// Timeout for flushing data from streaming storages.
func (s *Settings) StreamFlushIntervalMs(v time.Duration) {
	s.configs["stream_flush_interval_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// StreamPollTimeoutMs set stream_poll_timeout_ms setting
// Timeout for polling data from/to streaming storages.
func (s *Settings) StreamPollTimeoutMs(v time.Duration) {
	s.configs["stream_poll_timeout_ms"] = uint64(v.Milliseconds())
	s.dirty = true
}

// InsertAllowMaterializedColumns set insert_allow_materialized_columns setting
// If setting is enabled, Allow materialized columns in INSERT.
func (s *Settings) InsertAllowMaterializedColumns(v bool) {
	s.configs["insert_allow_materialized_columns"] = v
	s.dirty = true
}

// OptimizeThrowIfNoop set optimize_throw_if_noop setting
// If setting is enabled and OPTIMIZE query didn't actually assign a merge then an
// explanatory exception is thrown
func (s *Settings) OptimizeThrowIfNoop(v bool) {
	s.configs["optimize_throw_if_noop"] = v
	s.dirty = true
}

// UseIndexForInWithSubqueries set use_index_for_in_with_subqueries setting
// Try using an index if there is a subquery or a table expression on the right
// side of the IN operator.
func (s *Settings) UseIndexForInWithSubqueries(v bool) {
	s.configs["use_index_for_in_with_subqueries"] = v
	s.dirty = true
}

// JoinedSubqueryRequiresAlias set joined_subquery_requires_alias setting
// Force joined subqueries and table functions to have aliases for correct name
// qualification.
func (s *Settings) JoinedSubqueryRequiresAlias(v bool) {
	s.configs["joined_subquery_requires_alias"] = v
	s.dirty = true
}

// EmptyResultForAggregationByEmptySet set empty_result_for_aggregation_by_empty_set setting
// Return empty result when aggregating without keys on empty set.
func (s *Settings) EmptyResultForAggregationByEmptySet(v bool) {
	s.configs["empty_result_for_aggregation_by_empty_set"] = v
	s.dirty = true
}

// AllowDistributedDDL set allow_distributed_ddl setting
// If it is set to true, then a user is allowed to executed distributed DDL
// queries.
func (s *Settings) AllowDistributedDDL(v bool) {
	s.configs["allow_distributed_ddl"] = v
	s.dirty = true
}

// AllowSuspiciousCodecs set allow_suspicious_codecs setting
// If it is set to true, allow to specify meaningless compression codecs.
func (s *Settings) AllowSuspiciousCodecs(v bool) {
	s.configs["allow_suspicious_codecs"] = v
	s.dirty = true
}

// OdbcMaxFieldSize set odbc_max_field_size setting
// Max size of filed can be read from ODBC dictionary. Long strings are truncated.
func (s *Settings) OdbcMaxFieldSize(v uint64) {
	s.configs["odbc_max_field_size"] = v
	s.dirty = true
}

// QueryProfilerRealTimePeriodNs set query_profiler_real_time_period_ns setting
// Period for real clock timer of query profiler (in nanoseconds). Set 0 value to
// turn off the real clock query profiler. Recommended value is at least 10000000
// (100 times a second) for single queries or 1000000000 (once a second) for
// cluster-wide profiling.
func (s *Settings) QueryProfilerRealTimePeriodNs(v uint64) {
	s.configs["query_profiler_real_time_period_ns"] = v
	s.dirty = true
}

// QueryProfilerCPUTimePeriodNs set query_profiler_cpu_time_period_ns setting
// Period for CPU clock timer of query profiler (in nanoseconds). Set 0 value to
// turn off the CPU clock query profiler. Recommended value is at least 10000000
// (100 times a second) for single queries or 1000000000 (once a second) for
// cluster-wide profiling.
func (s *Settings) QueryProfilerCPUTimePeriodNs(v uint64) {
	s.configs["query_profiler_cpu_time_period_ns"] = v
	s.dirty = true
}

// MetricsPerfEventsEnabled set metrics_perf_events_enabled setting
// If enabled, some of the perf events will be measured throughout queries'
// execution.
func (s *Settings) MetricsPerfEventsEnabled(v bool) {
	s.configs["metrics_perf_events_enabled"] = v
	s.dirty = true
}

// MaxRowsToRead set max_rows_to_read setting
// Limit on read rows from the most 'deep' sources. That is, only in the deepest
// subquery. When reading from a remote server, it is only checked on a remote
// server.
func (s *Settings) MaxRowsToRead(v uint64) {
	s.configs["max_rows_to_read"] = v
	s.dirty = true
}

// MaxBytesToRead set max_bytes_to_read setting
// Limit on read bytes (after decompression) from the most 'deep' sources. That is,
// only in the deepest subquery. When reading from a remote server, it is only
// checked on a remote server.
func (s *Settings) MaxBytesToRead(v uint64) {
	s.configs["max_bytes_to_read"] = v
	s.dirty = true
}

// MaxRowsToGroupBy set max_rows_to_group_by setting
//
func (s *Settings) MaxRowsToGroupBy(v uint64) {
	s.configs["max_rows_to_group_by"] = v
	s.dirty = true
}

// MaxBytesBeforeExternalGroupBy set max_bytes_before_external_group_by setting
//
func (s *Settings) MaxBytesBeforeExternalGroupBy(v uint64) {
	s.configs["max_bytes_before_external_group_by"] = v
	s.dirty = true
}

// MaxRowsToSort set max_rows_to_sort setting
//
func (s *Settings) MaxRowsToSort(v uint64) {
	s.configs["max_rows_to_sort"] = v
	s.dirty = true
}

// MaxBytesToSort set max_bytes_to_sort setting
//
func (s *Settings) MaxBytesToSort(v uint64) {
	s.configs["max_bytes_to_sort"] = v
	s.dirty = true
}

// MaxBytesBeforeExternalSort set max_bytes_before_external_sort setting
//
func (s *Settings) MaxBytesBeforeExternalSort(v uint64) {
	s.configs["max_bytes_before_external_sort"] = v
	s.dirty = true
}

// MaxBytesBeforeRemergeSort set max_bytes_before_remerge_sort setting
// In case of ORDER BY with LIMIT, when memory usage is higher than specified
// threshold, perform additional steps of merging blocks before final merge to keep
// just top LIMIT rows.
func (s *Settings) MaxBytesBeforeRemergeSort(v uint64) {
	s.configs["max_bytes_before_remerge_sort"] = v
	s.dirty = true
}

// MaxResultRows set max_result_rows setting
// Limit on result size in rows. Also checked for intermediate data sent from
// remote servers.
func (s *Settings) MaxResultRows(v uint64) {
	s.configs["max_result_rows"] = v
	s.dirty = true
}

// MaxResultBytes set max_result_bytes setting
// Limit on result size in bytes (uncompressed). Also checked for intermediate data
// sent from remote servers.
func (s *Settings) MaxResultBytes(v uint64) {
	s.configs["max_result_bytes"] = v
	s.dirty = true
}

// MaxExecutionTime set max_execution_time setting
//
func (s *Settings) MaxExecutionTime(v time.Duration) {
	s.configs["max_execution_time"] = uint64(v.Seconds())
	s.dirty = true
}

// MinExecutionSpeed set min_execution_speed setting
// Minimum number of execution rows per second.
func (s *Settings) MinExecutionSpeed(v uint64) {
	s.configs["min_execution_speed"] = v
	s.dirty = true
}

// MaxExecutionSpeed set max_execution_speed setting
// Maximum number of execution rows per second.
func (s *Settings) MaxExecutionSpeed(v uint64) {
	s.configs["max_execution_speed"] = v
	s.dirty = true
}

// MinExecutionSpeedBytes set min_execution_speed_bytes setting
// Minimum number of execution bytes per second.
func (s *Settings) MinExecutionSpeedBytes(v uint64) {
	s.configs["min_execution_speed_bytes"] = v
	s.dirty = true
}

// MaxExecutionSpeedBytes set max_execution_speed_bytes setting
// Maximum number of execution bytes per second.
func (s *Settings) MaxExecutionSpeedBytes(v uint64) {
	s.configs["max_execution_speed_bytes"] = v
	s.dirty = true
}

// TimeoutBeforeCheckingExecutionSpeed set timeout_before_checking_execution_speed setting
// Check that the speed is not too low after the specified time has elapsed.
func (s *Settings) TimeoutBeforeCheckingExecutionSpeed(v time.Duration) {
	s.configs["timeout_before_checking_execution_speed"] = uint64(v.Seconds())
	s.dirty = true
}

// MaxColumnsToRead set max_columns_to_read setting
//
func (s *Settings) MaxColumnsToRead(v uint64) {
	s.configs["max_columns_to_read"] = v
	s.dirty = true
}

// MaxTemporaryColumns set max_temporary_columns setting
//
func (s *Settings) MaxTemporaryColumns(v uint64) {
	s.configs["max_temporary_columns"] = v
	s.dirty = true
}

// MaxTemporaryNonConstColumns set max_temporary_non_const_columns setting
//
func (s *Settings) MaxTemporaryNonConstColumns(v uint64) {
	s.configs["max_temporary_non_const_columns"] = v
	s.dirty = true
}

// MaxSubqueryDepth set max_subquery_depth setting
//
func (s *Settings) MaxSubqueryDepth(v uint64) {
	s.configs["max_subquery_depth"] = v
	s.dirty = true
}

// MaxPipelineDepth set max_pipeline_depth setting
//
func (s *Settings) MaxPipelineDepth(v uint64) {
	s.configs["max_pipeline_depth"] = v
	s.dirty = true
}

// MaxAstDepth set max_ast_depth setting
// Maximum depth of query syntax tree. Checked after parsing.
func (s *Settings) MaxAstDepth(v uint64) {
	s.configs["max_ast_depth"] = v
	s.dirty = true
}

// MaxAstElements set max_ast_elements setting
// Maximum size of query syntax tree in number of nodes. Checked after parsing.
func (s *Settings) MaxAstElements(v uint64) {
	s.configs["max_ast_elements"] = v
	s.dirty = true
}

// MaxExpandedAstElements set max_expanded_ast_elements setting
// Maximum size of query syntax tree in number of nodes after expansion of aliases
// and the asterisk.
func (s *Settings) MaxExpandedAstElements(v uint64) {
	s.configs["max_expanded_ast_elements"] = v
	s.dirty = true
}

// Readonly set readonly setting
// 0 - everything is allowed. 1 - only read requests. 2 - only read requests, as
// well as changing settings, except for the 'readonly' setting.
func (s *Settings) Readonly(v uint64) {
	s.configs["readonly"] = v
	s.dirty = true
}

// MaxRowsInSet set max_rows_in_set setting
// Maximum size of the set (in number of elements) resulting from the execution of
// the IN section.
func (s *Settings) MaxRowsInSet(v uint64) {
	s.configs["max_rows_in_set"] = v
	s.dirty = true
}

// MaxBytesInSet set max_bytes_in_set setting
// Maximum size of the set (in bytes in memory) resulting from the execution of the
// IN section.
func (s *Settings) MaxBytesInSet(v uint64) {
	s.configs["max_bytes_in_set"] = v
	s.dirty = true
}

// MaxRowsInJoin set max_rows_in_join setting
// Maximum size of the hash table for JOIN (in number of rows).
func (s *Settings) MaxRowsInJoin(v uint64) {
	s.configs["max_rows_in_join"] = v
	s.dirty = true
}

// MaxBytesInJoin set max_bytes_in_join setting
// Maximum size of the hash table for JOIN (in number of bytes in memory).
func (s *Settings) MaxBytesInJoin(v uint64) {
	s.configs["max_bytes_in_join"] = v
	s.dirty = true
}

// JoinAnyTakeLastRow set join_any_take_last_row setting
// When disabled (default) ANY JOIN will take the first found row for a key. When
// enabled, it will take the last row seen if there are multiple rows for the same
// key.
func (s *Settings) JoinAnyTakeLastRow(v bool) {
	s.configs["join_any_take_last_row"] = v
	s.dirty = true
}

// PartialMergeJoinOptimizations set partial_merge_join_optimizations setting
// Enable optimizations in partial merge join
func (s *Settings) PartialMergeJoinOptimizations(v bool) {
	s.configs["partial_merge_join_optimizations"] = v
	s.dirty = true
}

// DefaultMaxBytesInJoin set default_max_bytes_in_join setting
// Maximum size of right-side table if limit is required but max_bytes_in_join is
// not set.
func (s *Settings) DefaultMaxBytesInJoin(v uint64) {
	s.configs["default_max_bytes_in_join"] = v
	s.dirty = true
}

// PartialMergeJoinLeftTableBufferBytes set partial_merge_join_left_table_buffer_bytes setting
// If not 0 group left table blocks in bigger ones for left-side table in partial
// merge join. It uses up to 2x of specified memory per joining thread. In current
// version work only with 'partial_merge_join_optimizations = 1'.
func (s *Settings) PartialMergeJoinLeftTableBufferBytes(v uint64) {
	s.configs["partial_merge_join_left_table_buffer_bytes"] = v
	s.dirty = true
}

// PartialMergeJoinRowsInRightBlocks set partial_merge_join_rows_in_right_blocks setting
// Split right-hand joining data in blocks of specified size. It's a portion of
// data indexed by min-max values and possibly unloaded on disk.
func (s *Settings) PartialMergeJoinRowsInRightBlocks(v uint64) {
	s.configs["partial_merge_join_rows_in_right_blocks"] = v
	s.dirty = true
}

// JoinOnDiskMaxFilesToMerge set join_on_disk_max_files_to_merge setting
// For MergeJoin on disk set how much files it's allowed to sort simultaneously.
// Then this value bigger then more memory used and then less disk I/O needed.
// Minimum is 2.
func (s *Settings) JoinOnDiskMaxFilesToMerge(v uint64) {
	s.configs["join_on_disk_max_files_to_merge"] = v
	s.dirty = true
}

// MaxRowsToTransfer set max_rows_to_transfer setting
// Maximum size (in rows) of the transmitted external table obtained when the
// GLOBAL IN/JOIN section is executed.
func (s *Settings) MaxRowsToTransfer(v uint64) {
	s.configs["max_rows_to_transfer"] = v
	s.dirty = true
}

// MaxBytesToTransfer set max_bytes_to_transfer setting
// Maximum size (in uncompressed bytes) of the transmitted external table obtained
// when the GLOBAL IN/JOIN section is executed.
func (s *Settings) MaxBytesToTransfer(v uint64) {
	s.configs["max_bytes_to_transfer"] = v
	s.dirty = true
}

// MaxRowsInDistinct set max_rows_in_distinct setting
// Maximum number of elements during execution of DISTINCT.
func (s *Settings) MaxRowsInDistinct(v uint64) {
	s.configs["max_rows_in_distinct"] = v
	s.dirty = true
}

// MaxBytesInDistinct set max_bytes_in_distinct setting
// Maximum total size of state (in uncompressed bytes) in memory for the execution
// of DISTINCT.
func (s *Settings) MaxBytesInDistinct(v uint64) {
	s.configs["max_bytes_in_distinct"] = v
	s.dirty = true
}

// MaxMemoryUsage set max_memory_usage setting
// Maximum memory usage for processing of single query. Zero means unlimited.
func (s *Settings) MaxMemoryUsage(v uint64) {
	s.configs["max_memory_usage"] = v
	s.dirty = true
}

// MaxMemoryUsageForUser set max_memory_usage_for_user setting
// Maximum memory usage for processing all concurrently running queries for the
// user. Zero means unlimited.
func (s *Settings) MaxMemoryUsageForUser(v uint64) {
	s.configs["max_memory_usage_for_user"] = v
	s.dirty = true
}

// MemoryProfilerStep set memory_profiler_step setting
// Whenever query memory usage becomes larger than every next step in number of
// bytes the memory profiler will collect the allocating stack trace. Zero means
// disabled memory profiler. Values lower than a few megabytes will slow down query
// processing.
func (s *Settings) MemoryProfilerStep(v uint64) {
	s.configs["memory_profiler_step"] = v
	s.dirty = true
}

// MaxNetworkBandwidth set max_network_bandwidth setting
// The maximum speed of data exchange over the network in bytes per second for a
// query. Zero means unlimited.
func (s *Settings) MaxNetworkBandwidth(v uint64) {
	s.configs["max_network_bandwidth"] = v
	s.dirty = true
}

// MaxNetworkBytes set max_network_bytes setting
// The maximum number of bytes (compressed) to receive or transmit over the network
// for execution of the query.
func (s *Settings) MaxNetworkBytes(v uint64) {
	s.configs["max_network_bytes"] = v
	s.dirty = true
}

// MaxNetworkBandwidthForUser set max_network_bandwidth_for_user setting
// The maximum speed of data exchange over the network in bytes per second for all
// concurrently running user queries. Zero means unlimited.
func (s *Settings) MaxNetworkBandwidthForUser(v uint64) {
	s.configs["max_network_bandwidth_for_user"] = v
	s.dirty = true
}

// MaxNetworkBandwidthForAllUsers set max_network_bandwidth_for_all_users setting
// The maximum speed of data exchange over the network in bytes per second for all
// concurrently running queries. Zero means unlimited.
func (s *Settings) MaxNetworkBandwidthForAllUsers(v uint64) {
	s.configs["max_network_bandwidth_for_all_users"] = v
	s.dirty = true
}

// LogProfileEvents set log_profile_events setting
// Log query performance statistics into the query_log and query_thread_log.
func (s *Settings) LogProfileEvents(v bool) {
	s.configs["log_profile_events"] = v
	s.dirty = true
}

// LogQuerySettings set log_query_settings setting
// Log query settings into the query_log.
func (s *Settings) LogQuerySettings(v bool) {
	s.configs["log_query_settings"] = v
	s.dirty = true
}

// LogQueryThreads set log_query_threads setting
// Log query threads into system.query_thread_log table. This setting have effect
// only when 'log_queries' is true.
func (s *Settings) LogQueryThreads(v bool) {
	s.configs["log_query_threads"] = v
	s.dirty = true
}

// EnableOptimizePredicateExpression set enable_optimize_predicate_expression setting
// If it is set to true, optimize predicates to subqueries.
func (s *Settings) EnableOptimizePredicateExpression(v bool) {
	s.configs["enable_optimize_predicate_expression"] = v
	s.dirty = true
}

// EnableOptimizePredicateExpressionToFinalSubquery set enable_optimize_predicate_expression_to_final_subquery setting
// Allow push predicate to final subquery.
func (s *Settings) EnableOptimizePredicateExpressionToFinalSubquery(v bool) {
	s.configs["enable_optimize_predicate_expression_to_final_subquery"] = v
	s.dirty = true
}

// LowCardinalityMaxDictionarySize set low_cardinality_max_dictionary_size setting
// Maximum size (in rows) of shared global dictionary for LowCardinality type.
func (s *Settings) LowCardinalityMaxDictionarySize(v uint64) {
	s.configs["low_cardinality_max_dictionary_size"] = v
	s.dirty = true
}

// LowCardinalityUseSingleDictionaryForPart set low_cardinality_use_single_dictionary_for_part setting
// LowCardinality type serialization setting. If is true, than will use additional
// keys when global dictionary overflows. Otherwise, will create several shared
// dictionaries.
func (s *Settings) LowCardinalityUseSingleDictionaryForPart(v bool) {
	s.configs["low_cardinality_use_single_dictionary_for_part"] = v
	s.dirty = true
}

// DecimalCheckOverflow set decimal_check_overflow setting
// Check overflow of decimal arithmetic/comparison operations
func (s *Settings) DecimalCheckOverflow(v bool) {
	s.configs["decimal_check_overflow"] = v
	s.dirty = true
}

// PreferLocalhostReplica set prefer_localhost_replica setting
// 1 - always send query to local replica, if it exists. 0 - choose replica to send
// query between local and remote ones according to load_balancing
func (s *Settings) PreferLocalhostReplica(v bool) {
	s.configs["prefer_localhost_replica"] = v
	s.dirty = true
}

// MaxFetchPartitionRetriesCount set max_fetch_partition_retries_count setting
// Amount of retries while fetching partition from another host.
func (s *Settings) MaxFetchPartitionRetriesCount(v uint64) {
	s.configs["max_fetch_partition_retries_count"] = v
	s.dirty = true
}

// CalculateTextStackTrace set calculate_text_stack_trace setting
// Calculate text stack trace in case of exceptions during query execution. This is
// the default. It requires symbol lookups that may slow down fuzzing tests when
// huge amount of wrong queries are executed. In normal cases you should not
// disable this option.
func (s *Settings) CalculateTextStackTrace(v bool) {
	s.configs["calculate_text_stack_trace"] = v
	s.dirty = true
}

// AllowDDL set allow_ddl setting
// If it is set to true, then a user is allowed to executed DDL queries.
func (s *Settings) AllowDDL(v bool) {
	s.configs["allow_ddl"] = v
	s.dirty = true
}

// ParallelViewProcessing set parallel_view_processing setting
// Enables pushing to attached views concurrently instead of sequentially.
func (s *Settings) ParallelViewProcessing(v bool) {
	s.configs["parallel_view_processing"] = v
	s.dirty = true
}

// EnableDebugQueries set enable_debug_queries setting
// Enables debug queries such as AST.
func (s *Settings) EnableDebugQueries(v bool) {
	s.configs["enable_debug_queries"] = v
	s.dirty = true
}

// EnableUnalignedArrayJoin set enable_unaligned_array_join setting
// Allow ARRAY JOIN with multiple arrays that have different sizes. When this
// settings is enabled, arrays will be resized to the longest one.
func (s *Settings) EnableUnalignedArrayJoin(v bool) {
	s.configs["enable_unaligned_array_join"] = v
	s.dirty = true
}

// OptimizeReadInOrder set optimize_read_in_order setting
// Enable ORDER BY optimization for reading data in corresponding order in
// MergeTree tables.
func (s *Settings) OptimizeReadInOrder(v bool) {
	s.configs["optimize_read_in_order"] = v
	s.dirty = true
}

// OptimizeAggregationInOrder set optimize_aggregation_in_order setting
// Enable GROUP BY optimization for aggregating data in corresponding order in
// MergeTree tables.
func (s *Settings) OptimizeAggregationInOrder(v bool) {
	s.configs["optimize_aggregation_in_order"] = v
	s.dirty = true
}

// ReadInOrderTwoLevelMergeThreshold set read_in_order_two_level_merge_threshold setting
// Minimal number of parts to read to run preliminary merge step during multithread
// reading in order of primary key.
func (s *Settings) ReadInOrderTwoLevelMergeThreshold(v uint64) {
	s.configs["read_in_order_two_level_merge_threshold"] = v
	s.dirty = true
}

// LowCardinalityAllowInNativeFormat set low_cardinality_allow_in_native_format setting
// Use LowCardinality type in Native format. Otherwise, convert LowCardinality
// columns to ordinary for select query, and convert ordinary columns to required
// LowCardinality for insert query.
func (s *Settings) LowCardinalityAllowInNativeFormat(v bool) {
	s.configs["low_cardinality_allow_in_native_format"] = v
	s.dirty = true
}

// ExternalTableFunctionsUseNulls set external_table_functions_use_nulls setting
// If it is set to true, external table functions will implicitly use Nullable type
// if needed. Otherwise NULLs will be substituted with default values. Currently
// supported only by 'mysql' and 'odbc' table functions.
func (s *Settings) ExternalTableFunctionsUseNulls(v bool) {
	s.configs["external_table_functions_use_nulls"] = v
	s.dirty = true
}

// AllowHyperscan set allow_hyperscan setting
// Allow functions that use Hyperscan library. Disable to avoid potentially long
// compilation times and excessive resource usage.
func (s *Settings) AllowHyperscan(v bool) {
	s.configs["allow_hyperscan"] = v
	s.dirty = true
}

// AllowSimdjson set allow_simdjson setting
// Allow using simdjson library in 'JSON*' functions if AVX2 instructions are
// available. If disabled rapidjson will be used.
func (s *Settings) AllowSimdjson(v bool) {
	s.configs["allow_simdjson"] = v
	s.dirty = true
}

// AllowIntrospectionFunctions set allow_introspection_functions setting
// Allow functions for introspection of ELF and DWARF for query profiling. These
// functions are slow and may impose security considerations.
func (s *Settings) AllowIntrospectionFunctions(v bool) {
	s.configs["allow_introspection_functions"] = v
	s.dirty = true
}

// MaxPartitionsPerInsertBlock set max_partitions_per_insert_block setting
// Limit maximum number of partitions in single INSERTed block. Zero means
// unlimited. Throw exception if the block contains too many partitions. This
// setting is a safety threshold, because using large number of partitions is a
// common misconception.
func (s *Settings) MaxPartitionsPerInsertBlock(v uint64) {
	s.configs["max_partitions_per_insert_block"] = v
	s.dirty = true
}

// CheckQuerySingleValueResult set check_query_single_value_result setting
// Return check query result as single 1/0 value
func (s *Settings) CheckQuerySingleValueResult(v bool) {
	s.configs["check_query_single_value_result"] = v
	s.dirty = true
}

// AllowDropDetached set allow_drop_detached setting
// Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries
func (s *Settings) AllowDropDetached(v bool) {
	s.configs["allow_drop_detached"] = v
	s.dirty = true
}

// DistributedReplicaErrorHalfLife set distributed_replica_error_half_life setting
// Time period reduces replica error counter by 2 times.
func (s *Settings) DistributedReplicaErrorHalfLife(v time.Duration) {
	s.configs["distributed_replica_error_half_life"] = uint64(v.Seconds())
	s.dirty = true
}

// DistributedReplicaErrorCap set distributed_replica_error_cap setting
// Max number of errors per replica, prevents piling up an incredible amount of
// errors if replica was offline for some time and allows it to be reconsidered in
// a shorter amount of time.
func (s *Settings) DistributedReplicaErrorCap(v uint64) {
	s.configs["distributed_replica_error_cap"] = v
	s.dirty = true
}

// DistributedReplicaMaxIgnoredErrors set distributed_replica_max_ignored_errors setting
// Number of errors that will be ignored while choosing replicas
func (s *Settings) DistributedReplicaMaxIgnoredErrors(v uint64) {
	s.configs["distributed_replica_max_ignored_errors"] = v
	s.dirty = true
}

// AllowExperimentalLiveView set allow_experimental_live_view setting
// Enable LIVE VIEW. Not mature enough.
func (s *Settings) AllowExperimentalLiveView(v bool) {
	s.configs["allow_experimental_live_view"] = v
	s.dirty = true
}

// LiveViewHeartbeatInterval set live_view_heartbeat_interval setting
// The heartbeat interval in seconds to indicate live query is alive.
func (s *Settings) LiveViewHeartbeatInterval(v time.Duration) {
	s.configs["live_view_heartbeat_interval"] = uint64(v.Seconds())
	s.dirty = true
}

// MaxLiveViewInsertBlocksBeforeRefresh set max_live_view_insert_blocks_before_refresh setting
// Limit maximum number of inserted blocks after which mergeable blocks are dropped
// and query is re-executed.
func (s *Settings) MaxLiveViewInsertBlocksBeforeRefresh(v uint64) {
	s.configs["max_live_view_insert_blocks_before_refresh"] = v
	s.dirty = true
}

// MinFreeDiskSpaceForTemporaryData set min_free_disk_space_for_temporary_data setting
// The minimum disk space to keep while writing temporary data used in external
// sorting and aggregation.
func (s *Settings) MinFreeDiskSpaceForTemporaryData(v uint64) {
	s.configs["min_free_disk_space_for_temporary_data"] = v
	s.dirty = true
}

// AllowExperimentalDatabaseAtomic set allow_experimental_database_atomic setting
// Allow to create database with Engine=Atomic.
func (s *Settings) AllowExperimentalDatabaseAtomic(v bool) {
	s.configs["allow_experimental_database_atomic"] = v
	s.dirty = true
}

// ShowTableUUIDInTableCreateQueryIfNotNil set show_table_uuid_in_table_create_query_if_not_nil setting
// For tables in databases with Engine=Atomic show UUID of the table in its CREATE
// query.
func (s *Settings) ShowTableUUIDInTableCreateQueryIfNotNil(v bool) {
	s.configs["show_table_uuid_in_table_create_query_if_not_nil"] = v
	s.dirty = true
}

// EnableScalarSubqueryOptimization set enable_scalar_subquery_optimization setting
// If it is set to true, prevent scalar subqueries from (de)serializing large
// scalar values and possibly avoid running the same subquery more than once.
func (s *Settings) EnableScalarSubqueryOptimization(v bool) {
	s.configs["enable_scalar_subquery_optimization"] = v
	s.dirty = true
}

// OptimizeTrivialCountQuery set optimize_trivial_count_query setting
// Process trivial 'SELECT count() FROM table' query from metadata.
func (s *Settings) OptimizeTrivialCountQuery(v bool) {
	s.configs["optimize_trivial_count_query"] = v
	s.dirty = true
}

// MutationsSync set mutations_sync setting
// Wait for synchronous execution of ALTER TABLE UPDATE/DELETE queries (mutations).
// 0 - execute asynchronously. 1 - wait current server. 2 - wait all replicas if
// they exist.
func (s *Settings) MutationsSync(v uint64) {
	s.configs["mutations_sync"] = v
	s.dirty = true
}

// OptimizeMoveFunctionsOutOfAny set optimize_move_functions_out_of_any setting
// Move functions out of aggregate functions 'any', 'anyLast'.
func (s *Settings) OptimizeMoveFunctionsOutOfAny(v bool) {
	s.configs["optimize_move_functions_out_of_any"] = v
	s.dirty = true
}

// OptimizeArithmeticOperationsInAggregateFunctions set optimize_arithmetic_operations_in_aggregate_functions setting
// Move arithmetic operations out of aggregation functions
func (s *Settings) OptimizeArithmeticOperationsInAggregateFunctions(v bool) {
	s.configs["optimize_arithmetic_operations_in_aggregate_functions"] = v
	s.dirty = true
}

// OptimizeDuplicateOrderByAndDistinct set optimize_duplicate_order_by_and_distinct setting
// Remove duplicate ORDER BY and DISTINCT if it's possible
func (s *Settings) OptimizeDuplicateOrderByAndDistinct(v bool) {
	s.configs["optimize_duplicate_order_by_and_distinct"] = v
	s.dirty = true
}

// OptimizeIfChainToMiltiif set optimize_if_chain_to_miltiif setting
// Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not
// beneficial for numeric types.
func (s *Settings) OptimizeIfChainToMiltiif(v bool) {
	s.configs["optimize_if_chain_to_miltiif"] = v
	s.dirty = true
}

// AllowExperimentalAlterMaterializedViewStructure set allow_experimental_alter_materialized_view_structure setting
// Allow atomic alter on Materialized views. Work in progress.
func (s *Settings) AllowExperimentalAlterMaterializedViewStructure(v bool) {
	s.configs["allow_experimental_alter_materialized_view_structure"] = v
	s.dirty = true
}

// EnableEarlyConstantFolding set enable_early_constant_folding setting
// Enable query optimization where we analyze function and subqueries results and
// rewrite query if there're constants there
func (s *Settings) EnableEarlyConstantFolding(v bool) {
	s.configs["enable_early_constant_folding"] = v
	s.dirty = true
}

// DeduplicateBlocksInDependentMaterializedViews set deduplicate_blocks_in_dependent_materialized_views setting
// Should deduplicate blocks for materialized views if the block is not a duplicate
// for the table. Use true to always deduplicate in dependent tables.
func (s *Settings) DeduplicateBlocksInDependentMaterializedViews(v bool) {
	s.configs["deduplicate_blocks_in_dependent_materialized_views"] = v
	s.dirty = true
}

// UseCompactFormatInDistributedPartsNames set use_compact_format_in_distributed_parts_names setting
// Changes format of directories names for distributed table insert parts.
func (s *Settings) UseCompactFormatInDistributedPartsNames(v bool) {
	s.configs["use_compact_format_in_distributed_parts_names"] = v
	s.dirty = true
}

// MultipleJoinsRewriterVersion set multiple_joins_rewriter_version setting
// 1 or 2. Second rewriter version knows about table columns and keep not clashed
// names as is.
func (s *Settings) MultipleJoinsRewriterVersion(v uint64) {
	s.configs["multiple_joins_rewriter_version"] = v
	s.dirty = true
}

// ValidatePolygons set validate_polygons setting
// Throw exception if polygon is invalid in function pointInPolygon (e.g.
// self-tangent, self-intersecting). If the setting is false, the function will
// accept invalid polygons but may silently return wrong result.
func (s *Settings) ValidatePolygons(v bool) {
	s.configs["validate_polygons"] = v
	s.dirty = true
}

// MaxParserDepth set max_parser_depth setting
// Maximum parser depth (recursion depth of recursive descend parser).
func (s *Settings) MaxParserDepth(v uint64) {
	s.configs["max_parser_depth"] = v
	s.dirty = true
}

// TemporaryLiveViewTimeout set temporary_live_view_timeout setting
// Timeout after which temporary live view is deleted.
func (s *Settings) TemporaryLiveViewTimeout(v time.Duration) {
	s.configs["temporary_live_view_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// TransformNullIn set transform_null_in setting
// If enabled, NULL values will be matched with 'IN' operator as if they are
// considered equal.
func (s *Settings) TransformNullIn(v bool) {
	s.configs["transform_null_in"] = v
	s.dirty = true
}

// AllowNondeterministicMutations set allow_nondeterministic_mutations setting
// Allow non-deterministic functions in ALTER UPDATE/ALTER DELETE statements
func (s *Settings) AllowNondeterministicMutations(v bool) {
	s.configs["allow_nondeterministic_mutations"] = v
	s.dirty = true
}

// LockAcquireTimeout set lock_acquire_timeout setting
// How long locking request should wait before failing
func (s *Settings) LockAcquireTimeout(v time.Duration) {
	s.configs["lock_acquire_timeout"] = uint64(v.Seconds())
	s.dirty = true
}

// MaterializeTTLAfterModify set materialize_ttl_after_modify setting
// Apply TTL for old data, after ALTER MODIFY TTL query
func (s *Settings) MaterializeTTLAfterModify(v bool) {
	s.configs["materialize_ttl_after_modify"] = v
	s.dirty = true
}

// AllowExperimentalGeoTypes set allow_experimental_geo_types setting
// Allow geo data types such as Point, Ring, Polygon, MultiPolygon
func (s *Settings) AllowExperimentalGeoTypes(v bool) {
	s.configs["allow_experimental_geo_types"] = v
	s.dirty = true
}

// DataTypeDefaultNullable set data_type_default_nullable setting
// Data types without NULL or NOT NULL will make Nullable
func (s *Settings) DataTypeDefaultNullable(v bool) {
	s.configs["data_type_default_nullable"] = v
	s.dirty = true
}

// CastKeepNullable set cast_keep_nullable setting
// CAST operator keep Nullable for result data type
func (s *Settings) CastKeepNullable(v bool) {
	s.configs["cast_keep_nullable"] = v
	s.dirty = true
}

// AllowExperimentalLowCardinalityType set allow_experimental_low_cardinality_type setting
// Obsolete setting, does nothing. Will be removed after 2019-08-13
func (s *Settings) AllowExperimentalLowCardinalityType(v bool) {
	s.configs["allow_experimental_low_cardinality_type"] = v
	s.dirty = true
}

// Compile set compile setting
// Whether query compilation is enabled. Will be removed after 2020-03-13
func (s *Settings) Compile(v bool) {
	s.configs["compile"] = v
	s.dirty = true
}

// MinCountToCompile set min_count_to_compile setting
// Obsolete setting, does nothing. Will be removed after 2020-03-13
func (s *Settings) MinCountToCompile(v uint64) {
	s.configs["min_count_to_compile"] = v
	s.dirty = true
}

// AllowExperimentalMultipleJoinsEmulation set allow_experimental_multiple_joins_emulation setting
// Obsolete setting, does nothing. Will be removed after 2020-05-31
func (s *Settings) AllowExperimentalMultipleJoinsEmulation(v bool) {
	s.configs["allow_experimental_multiple_joins_emulation"] = v
	s.dirty = true
}

// AllowExperimentalCrossToJoinConversion set allow_experimental_cross_to_join_conversion setting
// Obsolete setting, does nothing. Will be removed after 2020-05-31
func (s *Settings) AllowExperimentalCrossToJoinConversion(v bool) {
	s.configs["allow_experimental_cross_to_join_conversion"] = v
	s.dirty = true
}

// AllowExperimentalDataSkippingIndices set allow_experimental_data_skipping_indices setting
// Obsolete setting, does nothing. Will be removed after 2020-05-31
func (s *Settings) AllowExperimentalDataSkippingIndices(v bool) {
	s.configs["allow_experimental_data_skipping_indices"] = v
	s.dirty = true
}

// MergeTreeUniformReadDistribution set merge_tree_uniform_read_distribution setting
// Obsolete setting, does nothing. Will be removed after 2020-05-20
func (s *Settings) MergeTreeUniformReadDistribution(v bool) {
	s.configs["merge_tree_uniform_read_distribution"] = v
	s.dirty = true
}

// MarkCacheMinLifetime set mark_cache_min_lifetime setting
// Obsolete setting, does nothing. Will be removed after 2020-05-31
func (s *Settings) MarkCacheMinLifetime(v uint64) {
	s.configs["mark_cache_min_lifetime"] = v
	s.dirty = true
}

// PartialMergeJoin set partial_merge_join setting
// Obsolete. Use join_algorithm='prefer_partial_merge' instead.
func (s *Settings) PartialMergeJoin(v bool) {
	s.configs["partial_merge_join"] = v
	s.dirty = true
}

// MaxMemoryUsageForAllQueries set max_memory_usage_for_all_queries setting
// Obsolete. Will be removed after 2020-10-20
func (s *Settings) MaxMemoryUsageForAllQueries(v uint64) {
	s.configs["max_memory_usage_for_all_queries"] = v
	s.dirty = true
}

// ForceOptimizeSkipUnusedShardsNoNested set force_optimize_skip_unused_shards_no_nested setting
// Obsolete setting, does nothing. Will be removed after 2020-12-01. Use
// force_optimize_skip_unused_shards_nesting instead.
func (s *Settings) ForceOptimizeSkipUnusedShardsNoNested(v bool) {
	s.configs["force_optimize_skip_unused_shards_no_nested"] = v
	s.dirty = true
}

// ExperimentalUseProcessors set experimental_use_processors setting
// Obsolete setting, does nothing. Will be removed after 2020-11-29.
func (s *Settings) ExperimentalUseProcessors(v bool) {
	s.configs["experimental_use_processors"] = v
	s.dirty = true
}

// FormatCsvDelimiter set format_csv_delimiter setting
// The character to be considered as a delimiter in CSV data. If setting with a
// string, a string has to have a length of 1.
func (s *Settings) FormatCsvDelimiter(v byte) {
	s.configs["format_csv_delimiter"] = v
	s.dirty = true
}

// FormatCsvAllowSingleQuotes set format_csv_allow_single_quotes setting
// If it is set to true, allow strings in single quotes.
func (s *Settings) FormatCsvAllowSingleQuotes(v bool) {
	s.configs["format_csv_allow_single_quotes"] = v
	s.dirty = true
}

// FormatCsvAllowDoubleQuotes set format_csv_allow_double_quotes setting
// If it is set to true, allow strings in double quotes.
func (s *Settings) FormatCsvAllowDoubleQuotes(v bool) {
	s.configs["format_csv_allow_double_quotes"] = v
	s.dirty = true
}

// OutputFormatCsvCrlfEndOfLine set output_format_csv_crlf_end_of_line setting
// If it is set true, end of line in CSV format will be \\r\\n instead of \\n.
func (s *Settings) OutputFormatCsvCrlfEndOfLine(v bool) {
	s.configs["output_format_csv_crlf_end_of_line"] = v
	s.dirty = true
}

// InputFormatCsvUnquotedNullLiteralAsNull set input_format_csv_unquoted_null_literal_as_null setting
// Consider unquoted NULL literal as \\N
func (s *Settings) InputFormatCsvUnquotedNullLiteralAsNull(v bool) {
	s.configs["input_format_csv_unquoted_null_literal_as_null"] = v
	s.dirty = true
}

// InputFormatSkipUnknownFields set input_format_skip_unknown_fields setting
// Skip columns with unknown names from input data (it works for JSONEachRow,
// CSVWithNames, TSVWithNames and TSKV formats).
func (s *Settings) InputFormatSkipUnknownFields(v bool) {
	s.configs["input_format_skip_unknown_fields"] = v
	s.dirty = true
}

// InputFormatWithNamesUseHeader set input_format_with_names_use_header setting
// For TSVWithNames and CSVWithNames input formats this controls whether format
// parser is to assume that column data appear in the input exactly as they are
// specified in the header.
func (s *Settings) InputFormatWithNamesUseHeader(v bool) {
	s.configs["input_format_with_names_use_header"] = v
	s.dirty = true
}

// InputFormatImportNestedJSON set input_format_import_nested_json setting
// Map nested JSON data to nested tables (it works for JSONEachRow format).
func (s *Settings) InputFormatImportNestedJSON(v bool) {
	s.configs["input_format_import_nested_json"] = v
	s.dirty = true
}

// OptimizeAggregatorsOfGroupByKeys set optimize_aggregators_of_group_by_keys setting
// Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section
func (s *Settings) OptimizeAggregatorsOfGroupByKeys(v bool) {
	s.configs["optimize_aggregators_of_group_by_keys"] = v
	s.dirty = true
}

// InputFormatDefaultsForOmittedFields set input_format_defaults_for_omitted_fields setting
// For input data calculate default expressions for omitted fields (it works for
// JSONEachRow, CSV and TSV formats).
func (s *Settings) InputFormatDefaultsForOmittedFields(v bool) {
	s.configs["input_format_defaults_for_omitted_fields"] = v
	s.dirty = true
}

// InputFormatTsvEmptyAsDefault set input_format_tsv_empty_as_default setting
// Treat empty fields in TSV input as default values.
func (s *Settings) InputFormatTsvEmptyAsDefault(v bool) {
	s.configs["input_format_tsv_empty_as_default"] = v
	s.dirty = true
}

// InputFormatNullAsDefault set input_format_null_as_default setting
// For text input formats initialize null fields with default values if data type
// of this field is not nullable
func (s *Settings) InputFormatNullAsDefault(v bool) {
	s.configs["input_format_null_as_default"] = v
	s.dirty = true
}

// OptimizeGroupByFunctionKeys set optimize_group_by_function_keys setting
// Eliminates functions of other keys in GROUP BY section
func (s *Settings) OptimizeGroupByFunctionKeys(v bool) {
	s.configs["optimize_group_by_function_keys"] = v
	s.dirty = true
}

// InputFormatValuesInterpretExpressions set input_format_values_interpret_expressions setting
// For Values format: if the field could not be parsed by streaming parser, run SQL
// parser and try to interpret it as SQL expression.
func (s *Settings) InputFormatValuesInterpretExpressions(v bool) {
	s.configs["input_format_values_interpret_expressions"] = v
	s.dirty = true
}

// InputFormatValuesDeduceTemplatesOfExpressions set input_format_values_deduce_templates_of_expressions setting
// For Values format: if the field could not be parsed by streaming parser, run SQL
// parser, deduce template of the SQL expression, try to parse all rows using
// template and then interpret expression for all rows.
func (s *Settings) InputFormatValuesDeduceTemplatesOfExpressions(v bool) {
	s.configs["input_format_values_deduce_templates_of_expressions"] = v
	s.dirty = true
}

// InputFormatValuesAccurateTypesOfLiterals set input_format_values_accurate_types_of_literals setting
// For Values format: when parsing and interpreting expressions using template,
// check actual type of literal to avoid possible overflow and precision issues.
func (s *Settings) InputFormatValuesAccurateTypesOfLiterals(v bool) {
	s.configs["input_format_values_accurate_types_of_literals"] = v
	s.dirty = true
}

// InputFormatAvroAllowMissingFields set input_format_avro_allow_missing_fields setting
// For Avro/AvroConfluent format: when field is not found in schema use default
// value instead of error
func (s *Settings) InputFormatAvroAllowMissingFields(v bool) {
	s.configs["input_format_avro_allow_missing_fields"] = v
	s.dirty = true
}

// OutputFormatJSONQuote64bitIntegers set output_format_json_quote_64bit_integers setting
// Controls quoting of 64-bit integers in JSON output format.
func (s *Settings) OutputFormatJSONQuote64bitIntegers(v bool) {
	s.configs["output_format_json_quote_64bit_integers"] = v
	s.dirty = true
}

// OutputFormatJSONQuoteDenormals set output_format_json_quote_denormals setting
// Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.
func (s *Settings) OutputFormatJSONQuoteDenormals(v bool) {
	s.configs["output_format_json_quote_denormals"] = v
	s.dirty = true
}

// OutputFormatJSONEscapeForwardSlashes set output_format_json_escape_forward_slashes setting
// Controls escaping forward slashes for string outputs in JSON output format. This
// is intended for compatibility with JavaScript. Don't confuse with backslashes
// that are always escaped.
func (s *Settings) OutputFormatJSONEscapeForwardSlashes(v bool) {
	s.configs["output_format_json_escape_forward_slashes"] = v
	s.dirty = true
}

// OutputFormatPrettyMaxRows set output_format_pretty_max_rows setting
// Rows limit for Pretty formats.
func (s *Settings) OutputFormatPrettyMaxRows(v uint64) {
	s.configs["output_format_pretty_max_rows"] = v
	s.dirty = true
}

// OutputFormatPrettyMaxColumnPadWidth set output_format_pretty_max_column_pad_width setting
// Maximum width to pad all values in a column in Pretty formats.
func (s *Settings) OutputFormatPrettyMaxColumnPadWidth(v uint64) {
	s.configs["output_format_pretty_max_column_pad_width"] = v
	s.dirty = true
}

// OutputFormatPrettyMaxValueWidth set output_format_pretty_max_value_width setting
// Maximum width of value to display in Pretty formats. If greater - it will be
// cut.
func (s *Settings) OutputFormatPrettyMaxValueWidth(v uint64) {
	s.configs["output_format_pretty_max_value_width"] = v
	s.dirty = true
}

// OutputFormatPrettyColor set output_format_pretty_color setting
// Use ANSI escape sequences to paint colors in Pretty formats
func (s *Settings) OutputFormatPrettyColor(v bool) {
	s.configs["output_format_pretty_color"] = v
	s.dirty = true
}

// OutputFormatParquetRowGroupSize set output_format_parquet_row_group_size setting
// Row group size in rows.
func (s *Settings) OutputFormatParquetRowGroupSize(v uint64) {
	s.configs["output_format_parquet_row_group_size"] = v
	s.dirty = true
}

// OutputFormatTsvCrlfEndOfLine set output_format_tsv_crlf_end_of_line setting
// If it is set true, end of line in TSV format will be \\r\\n instead of \\n.
func (s *Settings) OutputFormatTsvCrlfEndOfLine(v bool) {
	s.configs["output_format_tsv_crlf_end_of_line"] = v
	s.dirty = true
}

// InputFormatAllowErrorsNum set input_format_allow_errors_num setting
// Maximum absolute amount of errors while reading text formats (like CSV, TSV). In
// case of error, if at least absolute or relative amount of errors is lower than
// corresponding value, will skip until next line and continue.
func (s *Settings) InputFormatAllowErrorsNum(v uint64) {
	s.configs["input_format_allow_errors_num"] = v
	s.dirty = true
}

// InputFormatAllowErrorsRatio set input_format_allow_errors_ratio setting
// Maximum relative amount of errors while reading text formats (like CSV, TSV). In
// case of error, if at least absolute or relative amount of errors is lower than
// corresponding value, will skip until next line and continue.
func (s *Settings) InputFormatAllowErrorsRatio(v string) {
	s.configs["input_format_allow_errors_ratio"] = v
	s.dirty = true
}

// FormatRegexpSkipUnmatched set format_regexp_skip_unmatched setting
// Skip lines unmatched by regular expression (for Regexp format
func (s *Settings) FormatRegexpSkipUnmatched(v bool) {
	s.configs["format_regexp_skip_unmatched"] = v
	s.dirty = true
}

// OutputFormatEnableStreaming set output_format_enable_streaming setting
// Enable streaming in output formats that support it.
func (s *Settings) OutputFormatEnableStreaming(v bool) {
	s.configs["output_format_enable_streaming"] = v
	s.dirty = true
}

// OutputFormatWriteStatistics set output_format_write_statistics setting
// Write statistics about read rows, bytes, time elapsed in suitable output
// formats.
func (s *Settings) OutputFormatWriteStatistics(v bool) {
	s.configs["output_format_write_statistics"] = v
	s.dirty = true
}
