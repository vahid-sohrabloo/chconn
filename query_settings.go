package chconn

import (
	"time"
)

// Setting of single query setting
type Setting struct {
	w *Writer
}

func NewSetting() *Setting {
	return &Setting{
		w: NewWriter(),
	}
}

type SettingLoadBalancing string

const (
	// among replicas with a minimum number of errors selected randomly
	SettingLoadBalancingRandom = SettingLoadBalancing("random")

	// a replica is selected among the replicas with the minimum number of errors
	// with the minimum number of distinguished characters in the replica name and local hostname
	SettingLoadBalancingNearestHostname = SettingLoadBalancing("nearest_hostname")

	// replicas are walked through strictly in order; the number of errors does not matter
	SettingLoadBalancingInIrder = SettingLoadBalancing("in_order")

	// if first replica one has higher number of errors,
	//   pick a random one from replicas with minimum number of errors
	SettingLoadBalancingFirstOrRandom = SettingLoadBalancing("first_or_random")
)

// Which rows should be included in TOTALS.
type SettingTotalsMode string

const (
	// Count HAVING for all read rows;
	// including those not in max_rows_to_group_by
	// and have not passed HAVING after grouping.
	SettingTotalsModeBeforeHaving = SettingTotalsMode("before_having")
	// Count on all rows except those that have not passed HAVING;
	//  that is, to include in TOTALS all the rows that did not pass max_rows_to_group_by.
	SettingTotalsModeAfterHavingExclusive = SettingTotalsMode("after_having_exclusive")
	// Include only the rows that passed and max_rows_to_group_by, and HAVING.
	SettingTotalsModeAfterHavingInclusive = SettingTotalsMode("after_having_inclusive")
	// Automatically select between INCLUSIVE and EXCLUSIVE,
	SettingTotalsModeAfterHavingAuto = SettingTotalsMode("after_having_auto")
)

type SettingDistributedProductMode string

const (
	// Disable
	SettingDistributedProductModeDeny = SettingDistributedProductMode("deny")
	// Convert to local query
	SettingDistributedProductModeLOCAL = SettingDistributedProductMode("local")
	// Convert to global query
	SettingDistributedProductModeGLOBAL = SettingDistributedProductMode("global")
	// Enable
	SettingDistributedProductModeALLOW = SettingDistributedProductMode("allow")
)

type SettingJoinStrictness string

const (
	// Query JOIN without strictness will throw Exception.
	SettingJoinStrictnessUnspecified = SettingJoinStrictness("")
	// Query JOIN without strictness -> ALL JOIN ...
	SettingJoinStrictnessAll = SettingJoinStrictness("ALL")
	// Query JOIN without strictness -> ANY JOIN ...
	SettingJoinStrictnessAny = SettingJoinStrictness("ANY")
)

type SettingOverflowModeGroupBy string

const (
	// Throw exception.
	SettingOverflowModeGroupByThrow = SettingOverflowModeGroupBy("throw")
	// Abort query execution, return what is.
	SettingOverflowModeGroupByBreak = SettingOverflowModeGroupBy("break")
	/** Only for GROUP BY: do not add new rows to the set,
	 * but continue to aggregate for keys that are already in the set.
	 */
	SettingOverflowModeGroupByAny = SettingOverflowModeGroupBy("any")
)

type SettingOverflowMode string

const (
	// Throw exception.
	SettingOverflowModeThrow = SettingOverflowMode("throw")
	// Abort query execution, return what is.
	SettingOverflowModeBreak = SettingOverflowMode("break")
)

type SettingDateTimeInputFormat string

const (
	// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
	SettingDateTimeInputFormatBasic = SettingDateTimeInputFormat("basic")
	// Use sophisticated rules to parse whatever possible.
	SettingDateTimeInputFormatBestEffort = SettingDateTimeInputFormat("best_effort")
)

type SettingLogsLevel string

const (
	// none log level
	SettingLogsLevelNone = SettingLogsLevel("none")
	// error log level
	SettingLogsLevelError = SettingLogsLevel("error")
	// warning log level
	SettingLogsLevelWarning = SettingLogsLevel("warning")
	// information log level
	SettingLogsLevelInformation = SettingLogsLevel("information")
	// debug log level
	SettingLogsLevelDebug = SettingLogsLevel("debug")
	// trace log level
	SettingLogsLevelTrace = SettingLogsLevel("trace")
)

// The actual size of the block to compress, if the uncompressed data less than maxCompressBlockSize is no less than this value and no less than the volume of data for one mark.
func (s *Setting) SetMinCompressBlockSize(value uint64) {
	s.w.String("min_compress_block_size")
	s.w.Uvarint(value)
}

// The maximum size of blocks of uncompressed data before compressing for writing to a table.
func (s *Setting) SetMaxCompressBlockSize(value uint64) {
	s.w.String("max_compress_block_size")
	s.w.Uvarint(value)
}

// Maximum block size for reading
func (s *Setting) SetMaxBlockSize(value uint64) {
	s.w.String("max_block_size")
	s.w.Uvarint(value)
}

// The maximum block size for insertion, if we control the creation of blocks for insertion.
func (s *Setting) SetMaxInsertBlockSize(value uint64) {
	s.w.String("max_insert_block_size")
	s.w.Uvarint(value)
}

// Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.
func (s *Setting) SetMinInsertBlockSizeRows(value uint64) {
	s.w.String("min_insert_block_size_rows")
	s.w.Uvarint(value)
}

// Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.
func (s *Setting) SetMinInsertBlockSizeBytes(value uint64) {
	s.w.String("min_insert_block_size_bytes")
	s.w.Uvarint(value)
}

// The maximum number of threads to execute the request. By default, it is determined automatically.
func (s *Setting) SetMaxThreads(value uint64) {
	s.w.String("max_threads")
	s.w.Uvarint(value)
}

// The maximum number of threads to execute the ALTER requests. By default, it is determined automatically.
func (s *Setting) SetMaxAlterThreads(value uint64) {
	s.w.String("max_alter_threads")
	s.w.Uvarint(value)
}

// The maximum size of the buffer to read from the filesystem.
func (s *Setting) SetMaxReadBufferSize(value uint64) {
	s.w.String("max_read_buffer_size")
	s.w.Uvarint(value)
}

// The maximum number of connections for distributed processing of one query (should be greater than maxThreads).
func (s *Setting) SetMaxDistributedConnections(value uint64) {
	s.w.String("max_distributed_connections")
	s.w.Uvarint(value)
}

// Which part of the query can be read into RAM for parsing (the remaining data for INSERT, if any, is read later)
func (s *Setting) SetMaxQuerySize(value uint64) {
	s.w.String("max_query_size")
	s.w.Uvarint(value)
}

// The interval in microseconds to check if the request is canceled, and to send progress info.
func (s *Setting) SetInteractiveDelay(value uint64) {
	s.w.String("interactive_delay")
	s.w.Uvarint(value)
}

// Connection timeout if there are no replicas.
func (s *Setting) SetConnectTimeout(value time.Duration) {
	s.w.String("connect_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// Connection timeout for selecting first healthy replica.
func (s *Setting) SetConnectTimeoutWithFailoverMs(value time.Duration) {
	s.w.String("connect_timeout_with_failover_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

//
func (s *Setting) SetReceiveTimeout(value time.Duration) {
	s.w.String("receive_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

//
func (s *Setting) SetSendTimeout(value time.Duration) {
	s.w.String("send_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes
func (s *Setting) SetTCPKeepAliveTimeout(value time.Duration) {
	s.w.String("tcp_keep_alive_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// The wait time in the request queue, if the number of concurrent requests exceeds the maximum.
func (s *Setting) SetQueueMaxWaitMs(value time.Duration) {
	s.w.String("queue_max_wait_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// The wait time when connection pool is full.
func (s *Setting) SetConnectionPoolMaxWaitMs(value time.Duration) {
	s.w.String("connection_pool_max_wait_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// The wait time for running query with the same queryId to finish when setting 'replaceRunningQuery' is active.
func (s *Setting) SetReplaceRunningQueryMaxWaitMs(value time.Duration) {
	s.w.String("replace_running_query_max_wait_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// The wait time for reading from Kafka before retry.
func (s *Setting) SetKafkaMaxWaitMs(value time.Duration) {
	s.w.String("kafka_max_wait_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Block at the query wait loop on the server for the specified number of seconds.
func (s *Setting) SetPollInterval(value uint64) {
	s.w.String("poll_interval")
	s.w.Uvarint(value)
}

// Close idle TCP connections after specified number of seconds.
func (s *Setting) SetIdleConnectionTimeout(value uint64) {
	s.w.String("idle_connection_timeout")
	s.w.Uvarint(value)
}

// Maximum number of connections with one remote server in the pool.
func (s *Setting) SetDistributedConnectionsPoolSize(value uint64) {
	s.w.String("distributed_connections_pool_size")
	s.w.Uvarint(value)
}

// The maximum number of attempts to connect to replicas.
func (s *Setting) SetConnectionsWithFailoverMaxTries(value uint64) {
	s.w.String("connections_with_failover_max_tries")
	s.w.Uvarint(value)
}

// The mininum size of part to upload during multipart upload to S3.
func (s *Setting) SetS3MinUploadPartSize(value uint64) {
	s.w.String("s3_min_upload_part_size")
	s.w.Uvarint(value)
}

// Calculate minimums and maximums of the result columns. They can be output in JSON-formats.
func (s *Setting) SetExtremes(value bool) {
	s.w.String("extremes")
	s.w.Bool(value)
}

// Whether to use the cache of uncompressed blocks.
func (s *Setting) SetUseUncompressedCache(value bool) {
	s.w.String("use_uncompressed_cache")
	s.w.Bool(value)
}

// Whether the running request should be canceled with the same id as the new one.
func (s *Setting) SetReplaceRunningQuery(value bool) {
	s.w.String("replace_running_query")
	s.w.Bool(value)
}

// Number of threads performing background work for tables (for example, merging in merge tree). Only has meaning at server startup.
func (s *Setting) SetBackgroundPoolSize(value uint64) {
	s.w.String("background_pool_size")
	s.w.Uvarint(value)
}

// Number of threads performing background tasks for replicated tables. Only has meaning at server startup.
func (s *Setting) SetBackgroundSchedulePoolSize(value uint64) {
	s.w.String("background_schedule_pool_size")
	s.w.Uvarint(value)
}

// Sleep time for StorageDistributed DirectoryMonitors, in case of any errors delay grows exponentially.
func (s *Setting) SetDistributedDirectoryMonitorSleepTimeMs(value time.Duration) {
	s.w.String("distributed_directory_monitor_sleep_time_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Maximum sleep time for StorageDistributed DirectoryMonitors, it limits exponential growth too.
func (s *Setting) SetDistributedDirectoryMonitorMaxSleepTimeMs(value time.Duration) {
	s.w.String("distributed_directory_monitor_max_sleep_time_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Should StorageDistributed DirectoryMonitors try to batch individual inserts into bigger ones.
func (s *Setting) SetDistributedDirectoryMonitorBatchInserts(value bool) {
	s.w.String("distributed_directory_monitor_batch_inserts")
	s.w.Bool(value)
}

// Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree.
func (s *Setting) SetOptimizeMoveToPrewhere(value bool) {
	s.w.String("optimize_move_to_prewhere")
	s.w.Bool(value)
}

// Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.
func (s *Setting) SetReplicationAlterPartitionsSync(value uint64) {
	s.w.String("replication_alter_partitions_sync")
	s.w.Uvarint(value)
}

// Wait for actions to change the table structure within the specified number of seconds. 0 - wait unlimited time.
func (s *Setting) SetReplicationAlterColumnsTimeout(value uint64) {
	s.w.String("replication_alter_columns_timeout")
	s.w.Uvarint(value)
}

// Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.
func (s *Setting) SetLoadBalancing(value SettingLoadBalancing) {
	s.w.String("load_balancing")
	s.w.String(string(value))
}

// How to calculate TOTALS when HAVING is present, as well as when maxRowsToGroupBy and groupByOverflowMode = ‘any’ are present.
func (s *Setting) SetTotalsMode(value SettingTotalsMode) {
	s.w.String("totals_mode")
	s.w.String(string(value))
}

// The threshold for totalsMode = 'auto'.
func (s *Setting) SetTotalsAutoThreshold(value float32) {
	s.w.String("totals_auto_threshold")
	s.w.Float32(value)
}

// In CREATE TABLE statement allows specifying LowCardinality modifier for types of small fixed size (8 or less). Enabling this may increase merge times and memory consumption.
func (s *Setting) SetAllowSuspiciousLowCardinalityTypes(value bool) {
	s.w.String("allow_suspicious_low_cardinality_types")
	s.w.Bool(value)
}

// Compile some scalar functions and operators to native code.
func (s *Setting) SetCompileExpressions(value bool) {
	s.w.String("compile_expressions")
	s.w.Bool(value)
}

// The number of structurally identical queries before they are compiled.
func (s *Setting) SetMinCountToCompile(value uint64) {
	s.w.String("min_count_to_compile")
	s.w.Uvarint(value)
}

// The number of identical expressions before they are JIT-compiled
func (s *Setting) SetMinCountToCompileExpression(value uint64) {
	s.w.String("min_count_to_compile_expression")
	s.w.Uvarint(value)
}

// From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.
func (s *Setting) SetGroupByTwoLevelThreshold(value uint64) {
	s.w.String("group_by_two_level_threshold")
	s.w.Uvarint(value)
}

// From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.
func (s *Setting) SetGroupByTwoLevelThresholdBytes(value uint64) {
	s.w.String("group_by_two_level_threshold_bytes")
	s.w.Uvarint(value)
}

// Is the memory-saving mode of distributed aggregation enabled.
func (s *Setting) SetDistributedAggregationMemoryEfficient(value bool) {
	s.w.String("distributed_aggregation_memory_efficient")
	s.w.Bool(value)
}

// Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'maxThreads'.
func (s *Setting) SetAggregationMemoryEfficientMergeThreads(value uint64) {
	s.w.String("aggregation_memory_efficient_merge_threads")
	s.w.Uvarint(value)
}

// The maximum number of replicas of each shard used when the query is executed. For consistency (to get different parts of the same partition), this option only works for the specified sampling key. The lag of the replicas is not controlled.
func (s *Setting) SetMaxParallelReplicas(value uint64) {
	s.w.String("max_parallel_replicas")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetParallelReplicasCount(value uint64) {
	s.w.String("parallel_replicas_count")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetParallelReplicaOffset(value uint64) {
	s.w.String("parallel_replica_offset")
	s.w.Uvarint(value)
}

// If 1, ClickHouse silently skips unavailable shards and nodes unresolvable through DNS. Shard is marked as unavailable when none of the replicas can be reached.
func (s *Setting) SetSkipUnavailableShards(value bool) {
	s.w.String("skip_unavailable_shards")
	s.w.Bool(value)
}

// Do not merge aggregation states from different servers for distributed query processing - in case it is for certain that there are different keys on different shards.
func (s *Setting) SetDistributedGroupByNoMerge(value bool) {
	s.w.String("distributed_group_by_no_merge")
	s.w.Bool(value)
}

// Assumes that data is distributed by shardingKey. Optimization to skip unused shards if SELECT query filters by shardingKey.
func (s *Setting) SetOptimizeSkipUnusedShards(value bool) {
	s.w.String("optimize_skip_unused_shards")
	s.w.Bool(value)
}

// If at least as many lines are read from one file, the reading can be parallelized.
func (s *Setting) SetMergeTreeMinRowsForConcurrentRead(value uint64) {
	s.w.String("merge_tree_min_rows_for_concurrent_read")
	s.w.Uvarint(value)
}

// If at least as many bytes are read from one file, the reading can be parallelized.
func (s *Setting) SetMergeTreeMinBytesForConcurrentRead(value uint64) {
	s.w.String("merge_tree_min_bytes_for_concurrent_read")
	s.w.Uvarint(value)
}

// You can skip reading more than that number of rows at the price of one seek per file.
func (s *Setting) SetMergeTreeMinRowsForSeek(value uint64) {
	s.w.String("merge_tree_min_rows_for_seek")
	s.w.Uvarint(value)
}

// You can skip reading more than that number of bytes at the price of one seek per file.
func (s *Setting) SetMergeTreeMinBytesForSeek(value uint64) {
	s.w.String("merge_tree_min_bytes_for_seek")
	s.w.Uvarint(value)
}

// If the index segment can contain the required keys, divide it into as many parts and recursively check them.
func (s *Setting) SetMergeTreeCoarseIndexGranularity(value uint64) {
	s.w.String("merge_tree_coarse_index_granularity")
	s.w.Uvarint(value)
}

// The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)
func (s *Setting) SetMergeTreeMaxRowsToUseCache(value uint64) {
	s.w.String("merge_tree_max_rows_to_use_cache")
	s.w.Uvarint(value)
}

// The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)
func (s *Setting) SetMergeTreeMaxBytesToUseCache(value uint64) {
	s.w.String("merge_tree_max_bytes_to_use_cache")
	s.w.Uvarint(value)
}

// Distribute read from MergeTree over threads evenly, ensuring stable average execution time of each thread within one read operation.
func (s *Setting) SetMergeTreeUniformReadDistribution(value bool) {
	s.w.String("merge_tree_uniform_read_distribution")
	s.w.Bool(value)
}

// The maximum number of rows in MySQL batch insertion of the MySQL storage engine
func (s *Setting) SetMysqlMaxRowsToInsert(value uint64) {
	s.w.String("mysql_max_rows_to_insert")
	s.w.Uvarint(value)
}

// The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization
func (s *Setting) SetOptimizeMinEqualityDisjunctionChainLength(value uint64) {
	s.w.String("optimize_min_equality_disjunction_chain_length")
	s.w.Uvarint(value)
}

// The minimum number of bytes for reading the data with ODIRECT option during SELECT queries execution. 0 - disabled.
func (s *Setting) SetMinBytesToUseDirectIo(value uint64) {
	s.w.String("min_bytes_to_use_direct_io")
	s.w.Uvarint(value)
}

// Throw an exception if there is a partition key in a table, and it is not used.
func (s *Setting) SetForceIndexByDate(value bool) {
	s.w.String("force_index_by_date")
	s.w.Bool(value)
}

// Throw an exception if there is primary key in a table, and it is not used.
func (s *Setting) SetForcePrimaryKey(value bool) {
	s.w.String("force_primary_key")
	s.w.Bool(value)
}

// If the maximum size of markCache is exceeded, delete only records older than markCacheMinLifetime seconds.
func (s *Setting) SetMarkCacheMinLifetime(value uint64) {
	s.w.String("mark_cache_min_lifetime")
	s.w.Uvarint(value)
}

// Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.
func (s *Setting) SetMaxStreamsToMaxThreadsRatio(value float32) {
	s.w.String("max_streams_to_max_threads_ratio")
	s.w.Float32(value)
}

// Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and especially helpful when merged tables differ in size.
func (s *Setting) SetMaxStreamsMultiplierForMergeTables(value float32) {
	s.w.String("max_streams_multiplier_for_merge_tables")
	s.w.Float32(value)
}

// Allows you to select the method of data compression when writing.
func (s *Setting) SetNetworkCompressionMethod(value string) {
	s.w.String("network_compression_method")
	s.w.String(value)
}

// Allows you to select the level of ZSTD compression.
func (s *Setting) SetNetworkZstdCompressionLevel(value int64) {
	s.w.String("network_zstd_compression_level")
	s.w.Varint(value)
}

// Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.
func (s *Setting) SetPriority(value uint64) {
	s.w.String("priority")
	s.w.Uvarint(value)
}

// If non zero - set corresponding 'nice' value for query processing threads. Can be used to adjust query priority for OS scheduler.
func (s *Setting) SetOsThreadPriority(value int64) {
	s.w.String("os_thread_priority")
	s.w.Varint(value)
}

// Log requests and write the log to the system table.
func (s *Setting) SetLogQueries(value bool) {
	s.w.String("log_queries")
	s.w.Bool(value)
}

// If query length is greater than specified threshold (in bytes), then cut query when writing to query log. Also limit length of printed query in ordinary text log.
func (s *Setting) SetLogQueriesCutToLength(value uint64) {
	s.w.String("log_queries_cut_to_length")
	s.w.Uvarint(value)
}

// How are distributed subqueries performed inside IN or JOIN sections?
func (s *Setting) SetDistributedProductMode(value SettingDistributedProductMode) {
	s.w.String("distributed_product_mode")
	s.w.String(string(value))
}

// The maximum number of concurrent requests per user.
func (s *Setting) SetMaxConcurrentQueriesForUser(value uint64) {
	s.w.String("max_concurrent_queries_for_user")
	s.w.Uvarint(value)
}

// For INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be preformed
func (s *Setting) SetInsertDeduplicate(value bool) {
	s.w.String("insert_deduplicate")
	s.w.Bool(value)
}

// For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled.
func (s *Setting) SetInsertQuorum(value uint64) {
	s.w.String("insert_quorum")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetInsertQuorumTimeout(value time.Duration) {
	s.w.String("insert_quorum_timeout")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// For SELECT queries from the replicated table, throw an exception if the replica does not have a chunk written with the quorum; do not read the parts that have not yet been written with the quorum.
func (s *Setting) SetSelectSequentialConsistency(value uint64) {
	s.w.String("select_sequential_consistency")
	s.w.Uvarint(value)
}

// The maximum number of different shards and the maximum number of replicas of one shard in the `remote` function.
func (s *Setting) SetTableFunctionRemoteMaxAddresses(value uint64) {
	s.w.String("table_function_remote_max_addresses")
	s.w.Uvarint(value)
}

// Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.
func (s *Setting) SetReadBackoffMinLatencyMs(value time.Duration) {
	s.w.String("read_backoff_min_latency_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.
func (s *Setting) SetReadBackoffMaxThroughput(value uint64) {
	s.w.String("read_backoff_max_throughput")
	s.w.Uvarint(value)
}

// Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.
func (s *Setting) SetReadBackoffMinIntervalBetweenEventsMs(value time.Duration) {
	s.w.String("read_backoff_min_interval_between_events_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.
func (s *Setting) SetReadBackoffMinEvents(value uint64) {
	s.w.String("read_backoff_min_events")
	s.w.Uvarint(value)
}

// For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.
func (s *Setting) SetMemoryTrackerFaultProbability(value float32) {
	s.w.String("memory_tracker_fault_probability")
	s.w.Float32(value)
}

// Compress the result if the client over HTTP said that it understands data compressed by gzip or deflate.
func (s *Setting) SetEnableHTTPCompression(value bool) {
	s.w.String("enable_http_compression")
	s.w.Bool(value)
}

// Compression level - used if the client on HTTP said that it understands data compressed by gzip or deflate.
func (s *Setting) SetHTTPZlibCompressionLevel(value int64) {
	s.w.String("http_zlib_compression_level")
	s.w.Varint(value)
}

// If you uncompress the POST data from the client compressed by the native format, do not check the checksum.
func (s *Setting) SetHTTPNativeCompressionDisableChecksummingOnDecompress(value bool) {
	s.w.String("http_native_compression_disable_checksumming_on_decompress")
	s.w.Bool(value)
}

// What aggregate function to use for implementation of count(DISTINCT ...)
func (s *Setting) SetCountDistinctImplementation(value string) {
	s.w.String("count_distinct_implementation")
	s.w.String(value)
}

// Write statistics about read rows, bytes, time elapsed in suitable output formats.
func (s *Setting) SetOutputFormatWriteStatistics(value bool) {
	s.w.String("output_format_write_statistics")
	s.w.Bool(value)
}

// Write add http CORS header.
func (s *Setting) SetAddHTTPCorsHeader(value bool) {
	s.w.String("add_http_cors_header")
	s.w.Bool(value)
}

// Max number of http GET redirects hops allowed. Make sure additional security measures are in place to prevent a malicious server to redirect your requests to unexpected services.
func (s *Setting) SetMaxHTTPGetRedirects(value uint64) {
	s.w.String("max_http_get_redirects")
	s.w.Uvarint(value)
}

// Skip columns with unknown names from input data (it works for JSONEachRow, CSVWithNames, TSVWithNames and TSKV formats).
func (s *Setting) SetInputFormatSkipUnknownFields(value bool) {
	s.w.String("input_format_skip_unknown_fields")
	s.w.Bool(value)
}

// For TSVWithNames and CSVWithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.
func (s *Setting) SetInputFormatWithNamesUseHeader(value bool) {
	s.w.String("input_format_with_names_use_header")
	s.w.Bool(value)
}

// Map nested JSON data to nested tables (it works for JSONEachRow format).
func (s *Setting) SetInputFormatImportNestedJSON(value bool) {
	s.w.String("input_format_import_nested_json")
	s.w.Bool(value)
}

// For input data calculate default expressions for omitted fields (it works for JSONEachRow, CSV and TSV formats).
func (s *Setting) SetInputFormatDefaultsForOmittedFields(value bool) {
	s.w.String("input_format_defaults_for_omitted_fields")
	s.w.Bool(value)
}

// Treat empty fields in TSV input as default values.
func (s *Setting) SetInputFormatTsvEmptyAsDefault(value bool) {
	s.w.String("input_format_tsv_empty_as_default")
	s.w.Bool(value)
}

// For text input formats initialize null fields with default values if data type of this field is not nullable
func (s *Setting) SetInputFormatNullAsDefault(value bool) {
	s.w.String("input_format_null_as_default")
	s.w.Bool(value)
}

// For Values format: if field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.
func (s *Setting) SetInputFormatValuesInterpretExpressions(value bool) {
	s.w.String("input_format_values_interpret_expressions")
	s.w.Bool(value)
}

// For Values format: if field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.
func (s *Setting) SetInputFormatValuesDeduceTemplatesOfExpressions(value bool) {
	s.w.String("input_format_values_deduce_templates_of_expressions")
	s.w.Bool(value)
}

// For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.
func (s *Setting) SetInputFormatValuesAccurateTypesOfLiterals(value bool) {
	s.w.String("input_format_values_accurate_types_of_literals")
	s.w.Bool(value)
}

// Controls quoting of 64-bit integers in JSON output format.
func (s *Setting) SetOutputFormatJSONQuote64bitIntegers(value bool) {
	s.w.String("output_format_json_quote_64bit_integers")
	s.w.Bool(value)
}

// Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.
func (s *Setting) SetOutputFormatJSONQuoteDenormals(value bool) {
	s.w.String("output_format_json_quote_denormals")
	s.w.Bool(value)
}

// Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.
func (s *Setting) SetOutputFormatJSONEscapeForwardSlashes(value bool) {
	s.w.String("output_format_json_escape_forward_slashes")
	s.w.Bool(value)
}

// Rows limit for Pretty formats.
func (s *Setting) SetOutputFormatPrettyMaxRows(value uint64) {
	s.w.String("output_format_pretty_max_rows")
	s.w.Uvarint(value)
}

// Maximum width to pad all values in a column in Pretty formats.
func (s *Setting) SetOutputFormatPrettyMaxColumnPadWidth(value uint64) {
	s.w.String("output_format_pretty_max_column_pad_width")
	s.w.Uvarint(value)
}

// Use ANSI escape sequences to paint colors in Pretty formats
func (s *Setting) SetOutputFormatPrettyColor(value bool) {
	s.w.String("output_format_pretty_color")
	s.w.Bool(value)
}

// Row group size in rows.
func (s *Setting) SetOutputFormatParquetRowGroupSize(value uint64) {
	s.w.String("output_format_parquet_row_group_size")
	s.w.Uvarint(value)
}

// Use client timezone for interpreting DateTime string values, instead of adopting server timezone.
func (s *Setting) SetUseClientTimeZone(value bool) {
	s.w.String("use_client_time_zone")
	s.w.Bool(value)
}

// Send progress notifications using X-ClickHouse-Progress headers. Some clients do not support high amount of HTTP headers (Python requests in particular), so it is disabled by default.
func (s *Setting) SetSendProgressInHTTPHeaders(value bool) {
	s.w.String("send_progress_in_http_headers")
	s.w.Bool(value)
}

// Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.
func (s *Setting) SetHTTPHeadersProgressIntervalMs(value uint64) {
	s.w.String("http_headers_progress_interval_ms")
	s.w.Uvarint(value)
}

// Do fsync after changing metadata for tables and databases (.sql files). Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem.
func (s *Setting) SetFsyncMetadata(value bool) {
	s.w.String("fsync_metadata")
	s.w.Bool(value)
}

// Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.
func (s *Setting) SetInputFormatAllowErrorsNum(value uint64) {
	s.w.String("input_format_allow_errors_num")
	s.w.Uvarint(value)
}

// Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.
func (s *Setting) SetInputFormatAllowErrorsRatio(value float32) {
	s.w.String("input_format_allow_errors_ratio")
	s.w.Float32(value)
}

// Use NULLs for non-joined rows of outer JOINs for types that can be inside Nullable. If false, use default value of corresponding columns data type.
func (s *Setting) SetJoinUseNulls(value bool) {
	s.w.String("join_use_nulls")
	s.w.Bool(value)
}

// Set default strictness in JOIN query. Possible values: empty string, 'ANY', 'ALL'. If empty, query without strictness will throw exception.
func (s *Setting) SetJoinDefaultStrictness(value SettingJoinStrictness) {
	s.w.String("join_default_strictness")
	s.w.String(string(value))
}

// Enable old ANY JOIN logic with many-to-one left-to-right table keys mapping for all ANY JOINs. It leads to confusing not equal results for 't1 ANY LEFT JOIN t2' and 't2 ANY RIGHT JOIN t1'. ANY RIGHT JOIN needs one-to-many keys maping to be consistent with LEFT one.
func (s *Setting) SetAnyJoinDistinctRightTableKeys(value bool) {
	s.w.String("any_join_distinct_right_table_keys")
	s.w.Bool(value)
}

//
func (s *Setting) SetPreferredBlockSizeBytes(value uint64) {
	s.w.String("preferred_block_size_bytes")
	s.w.Uvarint(value)
}

// If set, distributed queries of Replicated tables will choose servers with replication delay in seconds less than the specified value (not inclusive). Zero means do not take delay into account.
func (s *Setting) SetMaxReplicaDelayForDistributedQueries(value uint64) {
	s.w.String("max_replica_delay_for_distributed_queries")
	s.w.Uvarint(value)
}

// Suppose maxReplicaDelayForDistributedQueries is set and all replicas for the queried table are stale. If this setting is enabled, the query will be performed anyway, otherwise the error will be reported.
func (s *Setting) SetFallbackToStaleReplicasForDistributedQueries(value bool) {
	s.w.String("fallback_to_stale_replicas_for_distributed_queries")
	s.w.Bool(value)
}

// Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.
func (s *Setting) SetPreferredMaxColumnInBlockSizeBytes(value uint64) {
	s.w.String("preferred_max_column_in_block_size_bytes")
	s.w.Uvarint(value)
}

// If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster.
func (s *Setting) SetInsertDistributedSync(value bool) {
	s.w.String("insert_distributed_sync")
	s.w.Bool(value)
}

// Timeout for insert query into distributed. Setting is used only with insertDistributedSync enabled. Zero value means no timeout.
func (s *Setting) SetInsertDistributedTimeout(value uint64) {
	s.w.String("insert_distributed_timeout")
	s.w.Uvarint(value)
}

// Timeout for DDL query responses from all hosts in cluster. If a ddl request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite.
func (s *Setting) SetDistributedDdlTaskTimeout(value int64) {
	s.w.String("distributed_ddl_task_timeout")
	s.w.Varint(value)
}

// Timeout for flushing data from streaming storages.
func (s *Setting) SetStreamFlushIntervalMs(value time.Duration) {
	s.w.String("stream_flush_interval_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Timeout for polling data from/to streaming storages.
func (s *Setting) SetStreamPollTimeoutMs(value time.Duration) {
	s.w.String("stream_poll_timeout_ms")
	s.w.Uvarint(uint64(value.Milliseconds()))
}

// Schema identifier (used by schema-based formats)
func (s *Setting) SetFormatSchema(value string) {
	s.w.String("format_schema")
	s.w.String(value)
}

// Path to file which contains format string for result set (for Template format)
func (s *Setting) SetFormatTemplateResultset(value string) {
	s.w.String("format_template_resultset")
	s.w.String(value)
}

// Path to file which contains format string for rows (for Template format)
func (s *Setting) SetFormatTemplateRow(value string) {
	s.w.String("format_template_row")
	s.w.String(value)
}

// Delimiter between rows (for Template format)
func (s *Setting) SetFormatTemplateRowsBetweenDelimiter(value string) {
	s.w.String("format_template_rows_between_delimiter")
	s.w.String(value)
}

// Field escaping rule (for CustomSeparated format)
func (s *Setting) SetFormatCustomEscapingRule(value string) {
	s.w.String("format_custom_escaping_rule")
	s.w.String(value)
}

// Delimiter between fields (for CustomSeparated format)
func (s *Setting) SetFormatCustomFieldDelimiter(value string) {
	s.w.String("format_custom_field_delimiter")
	s.w.String(value)
}

// Delimiter before field of the first column (for CustomSeparated format)
func (s *Setting) SetFormatCustomRowBeforeDelimiter(value string) {
	s.w.String("format_custom_row_before_delimiter")
	s.w.String(value)
}

// Delimiter after field of the last column (for CustomSeparated format)
func (s *Setting) SetFormatCustomRowAfterDelimiter(value string) {
	s.w.String("format_custom_row_after_delimiter")
	s.w.String(value)
}

// Delimiter between rows (for CustomSeparated format)
func (s *Setting) SetFormatCustomRowBetweenDelimiter(value string) {
	s.w.String("format_custom_row_between_delimiter")
	s.w.String(value)
}

// Prefix before result set (for CustomSeparated format)
func (s *Setting) SetFormatCustomResultBeforeDelimiter(value string) {
	s.w.String("format_custom_result_before_delimiter")
	s.w.String(value)
}

// Suffix after result set (for CustomSeparated format)
func (s *Setting) SetFormatCustomResultAfterDelimiter(value string) {
	s.w.String("format_custom_result_after_delimiter")
	s.w.String(value)
}

// If setting is enabled, Allow materialized columns in INSERT.
func (s *Setting) SetInsertAllowMaterializedColumns(value bool) {
	s.w.String("insert_allow_materialized_columns")
	s.w.Bool(value)
}

// HTTP connection timeout.
func (s *Setting) SetHTTPConnectionTimeout(value time.Duration) {
	s.w.String("http_connection_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// HTTP send timeout
func (s *Setting) SetHTTPSendTimeout(value time.Duration) {
	s.w.String("http_send_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// HTTP receive timeout
func (s *Setting) SetHTTPReceiveTimeout(value time.Duration) {
	s.w.String("http_receive_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// If setting is enabled and OPTIMIZE query didn't actually assign a merge then an explanatory exception is thrown
func (s *Setting) SetOptimizeThrowIfNoop(value bool) {
	s.w.String("optimize_throw_if_noop")
	s.w.Bool(value)
}

// Try using an index if there is a subquery or a table expression on the right side of the IN operator.
func (s *Setting) SetUseIndexForInWithSubqueries(value bool) {
	s.w.String("use_index_for_in_with_subqueries")
	s.w.Bool(value)
}

// Force joined subqueries to have aliases for correct name qualification.
func (s *Setting) SetJoinedSubqueryRequiresAlias(value bool) {
	s.w.String("joined_subquery_requires_alias")
	s.w.Bool(value)
}

// Return empty result when aggregating without keys on empty set.
func (s *Setting) SetEmptyResultForAggregationByEmptySet(value bool) {
	s.w.String("empty_result_for_aggregation_by_empty_set")
	s.w.Bool(value)
}

// If it is set to true, then a user is allowed to executed distributed DDL queries.
func (s *Setting) SetAllowDistributedDdl(value bool) {
	s.w.String("allow_distributed_ddl")
	s.w.Bool(value)
}

// Max size of filed can be read from ODBC dictionary. Long strings are truncated.
func (s *Setting) SetOdbcMaxFieldSize(value uint64) {
	s.w.String("odbc_max_field_size")
	s.w.Uvarint(value)
}

// Highly experimental. Period for real clock timer of query profiler (in nanoseconds). Set 0 value to turn off real clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.
func (s *Setting) SetQueryProfilerRealTimePeriodNs(value uint64) {
	s.w.String("query_profiler_real_time_period_ns")
	s.w.Uvarint(value)
}

// Highly experimental. Period for CPU clock timer of query profiler (in nanoseconds). Set 0 value to turn off CPU clock query profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.
func (s *Setting) SetQueryProfilerCPUTimePeriodNs(value uint64) {
	s.w.String("query_profiler_cpu_time_period_ns")
	s.w.Uvarint(value)
}

// Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.
func (s *Setting) SetMaxRowsToRead(value uint64) {
	s.w.String("max_rows_to_read")
	s.w.Uvarint(value)
}

// Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.
func (s *Setting) SetMaxBytesToRead(value uint64) {
	s.w.String("max_bytes_to_read")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetReadOverflowMode(value SettingOverflowMode) {
	s.w.String("read_overflow_mode")
	s.w.String(string(value))
}

//
func (s *Setting) SetMaxRowsToGroupBy(value uint64) {
	s.w.String("max_rows_to_group_by")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetGroupByOverflowMode(value SettingOverflowModeGroupBy) {
	s.w.String("group_by_overflow_mode")
	s.w.String(string(value))
}

//
func (s *Setting) SetMaxBytesBeforeExternalGroupBy(value uint64) {
	s.w.String("max_bytes_before_external_group_by")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxRowsToSort(value uint64) {
	s.w.String("max_rows_to_sort")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxBytesToSort(value uint64) {
	s.w.String("max_bytes_to_sort")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetSortOverflowMode(value SettingOverflowMode) {
	s.w.String("sort_overflow_mode")
	s.w.String(string(value))
}

//
func (s *Setting) SetMaxBytesBeforeExternalSort(value uint64) {
	s.w.String("max_bytes_before_external_sort")
	s.w.Uvarint(value)
}

// In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.
func (s *Setting) SetMaxBytesBeforeRemergeSort(value uint64) {
	s.w.String("max_bytes_before_remerge_sort")
	s.w.Uvarint(value)
}

// Limit on result size in rows. Also checked for intermediate data sent from remote servers.
func (s *Setting) SetMaxResultRows(value uint64) {
	s.w.String("max_result_rows")
	s.w.Uvarint(value)
}

// Limit on result size in bytes (uncompressed). Also checked for intermediate data sent from remote servers.
func (s *Setting) SetMaxResultBytes(value uint64) {
	s.w.String("max_result_bytes")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetResultOverflowMode(value SettingOverflowMode) {
	s.w.String("result_overflow_mode")
	s.w.String(string(value))
}

func (s *Setting) SetMaxExecutionTime(value time.Duration) {
	s.w.String("max_execution_time")
	s.w.Uvarint(uint64(value.Seconds()))
}

// What to do when the limit is exceeded.
func (s *Setting) SetTimeoutOverflowMode(value SettingOverflowMode) {
	s.w.String("timeout_overflow_mode")
	s.w.String(string(value))
}

// Minimum number of execution rows per second.
func (s *Setting) SetMinExecutionSpeed(value uint64) {
	s.w.String("min_execution_speed")
	s.w.Uvarint(value)
}

// Maximum number of execution rows per second.
func (s *Setting) SetMaxExecutionSpeed(value uint64) {
	s.w.String("max_execution_speed")
	s.w.Uvarint(value)
}

// Minimum number of execution bytes per second.
func (s *Setting) SetMinExecutionSpeedBytes(value uint64) {
	s.w.String("min_execution_speed_bytes")
	s.w.Uvarint(value)
}

// Maximum number of execution bytes per second.
func (s *Setting) SetMaxExecutionSpeedBytes(value uint64) {
	s.w.String("max_execution_speed_bytes")
	s.w.Uvarint(value)
}

// Check that the speed is not too low after the specified time has elapsed.
func (s *Setting) SetTimeoutBeforeCheckingExecutionSpeed(value time.Duration) {
	s.w.String("timeout_before_checking_execution_speed")
	s.w.Uvarint(uint64(value.Seconds()))
}

//
func (s *Setting) SetMaxColumnsToRead(value uint64) {
	s.w.String("max_columns_to_read")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxTemporaryColumns(value uint64) {
	s.w.String("max_temporary_columns")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxTemporaryNonConstColumns(value uint64) {
	s.w.String("max_temporary_non_const_columns")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxSubqueryDepth(value uint64) {
	s.w.String("max_subquery_depth")
	s.w.Uvarint(value)
}

//
func (s *Setting) SetMaxPipelineDepth(value uint64) {
	s.w.String("max_pipeline_depth")
	s.w.Uvarint(value)
}

// Maximum depth of query syntax tree. Checked after parsing.
func (s *Setting) SetMaxAstDepth(value uint64) {
	s.w.String("max_ast_depth")
	s.w.Uvarint(value)
}

// Maximum size of query syntax tree in number of nodes. Checked after parsing.
func (s *Setting) SetMaxAstElements(value uint64) {
	s.w.String("max_ast_elements")
	s.w.Uvarint(value)
}

// Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.
func (s *Setting) SetMaxExpandedAstElements(value uint64) {
	s.w.String("max_expanded_ast_elements")
	s.w.Uvarint(value)
}

// 0 - everything is allowed. 1 - only read requests. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.
func (s *Setting) SetReadonly(value uint64) {
	s.w.String("readonly")
	s.w.Uvarint(value)
}

// Maximum size of the set (in number of elements) resulting from the execution of the IN section.
func (s *Setting) SetMaxRowsInSet(value uint64) {
	s.w.String("max_rows_in_set")
	s.w.Uvarint(value)
}

// Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.
func (s *Setting) SetMaxBytesInSet(value uint64) {
	s.w.String("max_bytes_in_set")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetOverflowMode(value SettingOverflowMode) {
	s.w.String("set_overflow_mode")
	s.w.String(string(value))
}

// Maximum size of the hash table for JOIN (in number of rows).
func (s *Setting) SetMaxRowsInJoin(value uint64) {
	s.w.String("max_rows_in_join")
	s.w.Uvarint(value)
}

// Maximum size of the hash table for JOIN (in number of bytes in memory).
func (s *Setting) SetMaxBytesInJoin(value uint64) {
	s.w.String("max_bytes_in_join")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetJoinOverflowMode(value SettingOverflowMode) {
	s.w.String("join_overflow_mode")
	s.w.String(string(value))
}

// When disabled (default) ANY JOIN will take the first found row for a key. When enabled, it will take the last row seen if there are multiple rows for the same key.
func (s *Setting) SetJoinAnyTakeLastRow(value bool) {
	s.w.String("join_any_take_last_row")
	s.w.Bool(value)
}

// Use partial merge join instead of hash join for LEFT and INNER JOINs.
func (s *Setting) SetPartialMergeJoin(value bool) {
	s.w.String("partial_merge_join")
	s.w.Bool(value)
}

// Enable optimizations in partial merge join
func (s *Setting) SetPartialMergeJoinOptimizations(value bool) {
	s.w.String("partial_merge_join_optimizations")
	s.w.Bool(value)
}

// Maximum size of right-side table if limit's required but maxBytesInJoin is not set.
func (s *Setting) SetDefaultMaxBytesInJoin(value uint64) {
	s.w.String("default_max_bytes_in_join")
	s.w.Uvarint(value)
}

// Split right-hand joining data in blocks of specified size. It's a portion of data indexed by min-max values and possibly unloaded on disk.
func (s *Setting) SetPartialMergeJoinRowsInRightBlocks(value uint64) {
	s.w.String("partial_merge_join_rows_in_right_blocks")
	s.w.Uvarint(value)
}

// Group left-hand joining data in bigger blocks. Setting it to a bigger value increase JOIN performance and memory usage.
func (s *Setting) SetPartialMergeJoinRowsInLeftBlocks(value uint64) {
	s.w.String("partial_merge_join_rows_in_left_blocks")
	s.w.Uvarint(value)
}

// Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.
func (s *Setting) SetMaxRowsToTransfer(value uint64) {
	s.w.String("max_rows_to_transfer")
	s.w.Uvarint(value)
}

// Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.
func (s *Setting) SetMaxBytesToTransfer(value uint64) {
	s.w.String("max_bytes_to_transfer")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetTransferOverflowMode(value SettingOverflowMode) {
	s.w.String("transfer_overflow_mode")
	s.w.String(string(value))
}

// Maximum number of elements during execution of DISTINCT.
func (s *Setting) SetMaxRowsInDistinct(value uint64) {
	s.w.String("max_rows_in_distinct")
	s.w.Uvarint(value)
}

// Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.
func (s *Setting) SetMaxBytesInDistinct(value uint64) {
	s.w.String("max_bytes_in_distinct")
	s.w.Uvarint(value)
}

// What to do when the limit is exceeded.
func (s *Setting) SetDistinctOverflowMode(value SettingOverflowMode) {
	s.w.String("distinct_overflow_mode")
	s.w.String(string(value))
}

// Maximum memory usage for processing of single query. Zero means unlimited.
func (s *Setting) SetMaxMemoryUsage(value uint64) {
	s.w.String("max_memory_usage")
	s.w.Uvarint(value)
}

// Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.
func (s *Setting) SetMaxMemoryUsageForUser(value uint64) {
	s.w.String("max_memory_usage_for_user")
	s.w.Uvarint(value)
}

// Maximum memory usage for processing all concurrently running queries on the server. Zero means unlimited.
func (s *Setting) SetMaxMemoryUsageForAllQueries(value uint64) {
	s.w.String("max_memory_usage_for_all_queries")
	s.w.Uvarint(value)
}

// The maximum speed of data exchange over the network in bytes per second for a query. Zero means unlimited.
func (s *Setting) SetMaxNetworkBandwidth(value uint64) {
	s.w.String("max_network_bandwidth")
	s.w.Uvarint(value)
}

// The maximum number of bytes (compressed) to receive or transmit over the network for execution of the query.
func (s *Setting) SetMaxNetworkBytes(value uint64) {
	s.w.String("max_network_bytes")
	s.w.Uvarint(value)
}

// The maximum speed of data exchange over the network in bytes per second for all concurrently running user queries. Zero means unlimited
func (s *Setting) SetMaxNetworkBandwidthForUser(value uint64) {
	s.w.String("max_network_bandwidth_for_user")
	s.w.Uvarint(value)
}

// The maximum speed of data exchange over the network in bytes per second for all concurrently running queries. Zero means unlimited.
func (s *Setting) SetMaxNetworkBandwidthForAllUsers(value uint64) {
	s.w.String("max_network_bandwidth_for_all_users")
	s.w.Uvarint(value)
}

// The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.
func (s *Setting) SetFormatCsvDelimiter(value byte) {
	s.w.String("format_csv_delimiter")
	s.w.Buffer([]byte{value})
}

// If it is set to true, allow strings in single quotes.
func (s *Setting) SetFormatCsvAllowSingleQuotes(value bool) {
	s.w.String("format_csv_allow_single_quotes")
	s.w.Bool(value)
}

// If it is set to true, allow strings in double quotes.
func (s *Setting) SetFormatCsvAllowDoubleQuotes(value bool) {
	s.w.String("format_csv_allow_double_quotes")
	s.w.Bool(value)
}

// Consider unquoted NULL literal as N
func (s *Setting) SetInputFormatCsvUnquotedNullLiteralAsNull(value bool) {
	s.w.String("input_format_csv_unquoted_null_literal_as_null")
	s.w.Bool(value)
}

// Method to read DateTime from text input formats. Possible values: 'basic' and 'bestEffort'.
func (s *Setting) SetDateTimeInputFormat(value SettingDateTimeInputFormat) {
	s.w.String("date_time_input_format")
	s.w.String(string(value))
}

// Log query performance statistics into the queryLog and queryThreadLog.
func (s *Setting) SetLogProfileEvents(value bool) {
	s.w.String("log_profile_events")
	s.w.Bool(value)
}

// Log query settings into the queryLog.
func (s *Setting) SetLogQuerySettings(value bool) {
	s.w.String("log_query_settings")
	s.w.Bool(value)
}

// Log query threads into system.queryThreadLog table. This setting have effect only when 'logQueries' is true.
func (s *Setting) SetLogQueryThreads(value bool) {
	s.w.String("log_query_threads")
	s.w.Bool(value)
}

// Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'none'
func (s *Setting) SetSendLogsLevel(value SettingLogsLevel) {
	s.w.String("send_logs_level")
	s.w.String(string(value))
}

// If it is set to true, optimize predicates to subqueries.
func (s *Setting) SetEnableOptimizePredicateExpression(value bool) {
	s.w.String("enable_optimize_predicate_expression")
	s.w.Bool(value)
}

// Allow push predicate to final subquery.
func (s *Setting) SetEnableOptimizePredicateExpressionToFinalSubquery(value bool) {
	s.w.String("enable_optimize_predicate_expression_to_final_subquery")
	s.w.Bool(value)
}

// Maximum size (in rows) of shared global dictionary for LowCardinality type.
func (s *Setting) SetLowCardinalityMaxDictionarySize(value uint64) {
	s.w.String("low_cardinality_max_dictionary_size")
	s.w.Uvarint(value)
}

// LowCardinality type serialization setting. If is true, than will use additional keys when global dictionary overflows. Otherwise, will create several shared dictionaries.
func (s *Setting) SetLowCardinalityUseSingleDictionaryForPart(value bool) {
	s.w.String("low_cardinality_use_single_dictionary_for_part")
	s.w.Bool(value)
}

// Check overflow of decimal arithmetic/comparison operations
func (s *Setting) SetDecimalCheckOverflow(value bool) {
	s.w.String("decimal_check_overflow")
	s.w.Bool(value)
}

// 1 - always send query to local replica, if it exists. 0 - choose replica to send query between local and remote ones according to loadBalancing
func (s *Setting) SetPreferLocalhostReplica(value bool) {
	s.w.String("prefer_localhost_replica")
	s.w.Bool(value)
}

// Amount of retries while fetching partition from another host.
func (s *Setting) SetMaxFetchPartitionRetriesCount(value uint64) {
	s.w.String("max_fetch_partition_retries_count")
	s.w.Uvarint(value)
}

// Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in user profile. Note that content is parsed and external tables are created in memory before start of query execution. And this is the only limit that has effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).
func (s *Setting) SetHTTPMaxMultipartFormDataSize(value uint64) {
	s.w.String("http_max_multipart_form_data_size")
	s.w.Uvarint(value)
}

// Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when huge amount of wrong queries are executed. In normal cases you should not disable this option.
func (s *Setting) SetCalculateTextStackTrace(value bool) {
	s.w.String("calculate_text_stack_trace")
	s.w.Bool(value)
}

// If it is set to true, then a user is allowed to executed DDL queries.
func (s *Setting) SetAllowDdl(value bool) {
	s.w.String("allow_ddl")
	s.w.Bool(value)
}

// Enables pushing to attached views concurrently instead of sequentially.
func (s *Setting) SetParallelViewProcessing(value bool) {
	s.w.String("parallel_view_processing")
	s.w.Bool(value)
}

// Enables debug queries such as AST.
func (s *Setting) SetEnableDebugQueries(value bool) {
	s.w.String("enable_debug_queries")
	s.w.Bool(value)
}

// Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.
func (s *Setting) SetEnableUnalignedArrayJoin(value bool) {
	s.w.String("enable_unaligned_array_join")
	s.w.Bool(value)
}

// Enable ORDER BY optimization for reading data in corresponding order in MergeTree tables.
func (s *Setting) SetOptimizeReadInOrder(value bool) {
	s.w.String("optimize_read_in_order")
	s.w.Bool(value)
}

// Use LowCardinality type in Native format. Otherwise, convert LowCardinality columns to ordinary for select query, and convert ordinary columns to required LowCardinality for insert query.
func (s *Setting) SetLowCardinalityAllowInNativeFormat(value bool) {
	s.w.String("low_cardinality_allow_in_native_format")
	s.w.Bool(value)
}

// Emulate multiple joins using subselects
func (s *Setting) SetAllowExperimentalMultipleJoinsEmulation(value bool) {
	s.w.String("allow_experimental_multiple_joins_emulation")
	s.w.Bool(value)
}

// Convert CROSS JOIN to INNER JOIN if possible
func (s *Setting) SetAllowExperimentalCrossToJoinConversion(value bool) {
	s.w.String("allow_experimental_cross_to_join_conversion")
	s.w.Bool(value)
}

// Cancel HTTP readonly queries when a client closes the connection without waiting for response.
func (s *Setting) SetCancelHTTPReadonlyQueriesOnClientClose(value bool) {
	s.w.String("cancel_http_readonly_queries_on_client_close")
	s.w.Bool(value)
}

// If it is set to true, external table functions will implicitly use Nullable type if needed. Otherwise NULLs will be substituted with default values. Currently supported only by 'mysql' and 'odbc' table functions.
func (s *Setting) SetExternalTableFunctionsUseNulls(value bool) {
	s.w.String("external_table_functions_use_nulls")
	s.w.Bool(value)
}

// If it is set to true, data skipping indices can be used in CREATE TABLE/ALTER TABLE queries.
func (s *Setting) SetAllowExperimentalDataSkippingIndices(value bool) {
	s.w.String("allow_experimental_data_skipping_indices")
	s.w.Bool(value)
}

// Use processors pipeline.
func (s *Setting) SetExperimentalUseProcessors(value bool) {
	s.w.String("experimental_use_processors")
	s.w.Bool(value)
}

// Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.
func (s *Setting) SetAllowHyperscan(value bool) {
	s.w.String("allow_hyperscan")
	s.w.Bool(value)
}

// Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.
func (s *Setting) SetAllowSimdjson(value bool) {
	s.w.String("allow_simdjson")
	s.w.Bool(value)
}

// Allow functions for introspection of ELF and DWARF for query profiling. These functions are slow and may impose security considerations.
func (s *Setting) SetAllowIntrospectionFunctions(value bool) {
	s.w.String("allow_introspection_functions")
	s.w.Bool(value)
}

// Limit maximum number of partitions in single INSERTed block. Zero means unlimited. Throw exception if the block contains too many partitions. This setting is a safety threshold, because using large number of partitions is a common misconception.
func (s *Setting) SetMaxPartitionsPerInsertBlock(value uint64) {
	s.w.String("max_partitions_per_insert_block")
	s.w.Uvarint(value)
}

// Return check query result as single 1/0 value
func (s *Setting) SetCheckQuerySingleValueResult(value bool) {
	s.w.String("check_query_single_value_result")
	s.w.Bool(value)
}

// Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries
func (s *Setting) SetAllowDropDetached(value bool) {
	s.w.String("allow_drop_detached")
	s.w.Bool(value)
}

// Time period reduces replica error counter by 2 times.
func (s *Setting) SetDistributedReplicaErrorHalfLife(value time.Duration) {
	s.w.String("distributed_replica_error_half_life")
	s.w.Uvarint(uint64(value.Seconds()))
}

// Max number of errors per replica, prevents piling up incredible amount of errors if replica was offline for some time and allows it to be reconsidered in a shorter amount of time.
func (s *Setting) SetDistributedReplicaErrorCap(value uint64) {
	s.w.String("distributed_replica_error_cap")
	s.w.Uvarint(value)
}

// Enable LIVE VIEW. Not mature enough.
func (s *Setting) SetAllowExperimentalLiveView(value bool) {
	s.w.String("allow_experimental_live_view")
	s.w.Bool(value)
}

// The heartbeat interval in seconds to indicate live query is alive.
func (s *Setting) SetLiveViewHeartbeatInterval(value time.Duration) {
	s.w.String("live_view_heartbeat_interval")
	s.w.Uvarint(uint64(value.Seconds()))
}

// Timeout after which temporary live view is deleted.
func (s *Setting) SetTemporaryLiveViewTimeout(value time.Duration) {
	s.w.String("temporary_live_view_timeout")
	s.w.Uvarint(uint64(value.Seconds()))
}

// Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.
func (s *Setting) SetMaxLiveViewInsertBlocksBeforeRefresh(value uint64) {
	s.w.String("max_live_view_insert_blocks_before_refresh")
	s.w.Uvarint(value)
}

// The minimum disk space to keep while writing temporary data used in external sorting and aggregation.
func (s *Setting) SetMinFreeDiskSpaceForTemporaryData(value uint64) {
	s.w.String("min_free_disk_space_for_temporary_data")
	s.w.Uvarint(value)
}

// If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.
func (s *Setting) SetEnableScalarSubqueryOptimization(value bool) {
	s.w.String("enable_scalar_subquery_optimization")
	s.w.Bool(value)
}

// Process trivial 'SELECT count() FROM table' query from metadata.
func (s *Setting) SetOptimizeTrivialCountQuery(value bool) {
	s.w.String("optimize_trivial_count_query")
	s.w.Bool(value)
}
