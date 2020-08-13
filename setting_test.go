package chconn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSetting(t *testing.T) {
	durExample, _ := time.ParseDuration("4s")
	t.Run("min_compress_block_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MinCompressBlockSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_compress_block_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_compress_block_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxCompressBlockSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_compress_block_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_block_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBlockSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_block_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_insert_block_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxInsertBlockSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_insert_block_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_insert_block_size_rows", func(t *testing.T) {
		setting := NewSettings()
		setting.MinInsertBlockSizeRows(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_insert_block_size_rows")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_insert_block_size_rows_for_materialized_views", func(t *testing.T) {
		setting := NewSettings()
		setting.MinInsertBlockSizeRowsForMaterializedViews(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_insert_block_size_rows_for_materialized_views")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_insert_block_size_bytes_for_materialized_views", func(t *testing.T) {
		setting := NewSettings()
		setting.MinInsertBlockSizeBytesForMaterializedViews(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_insert_block_size_bytes_for_materialized_views")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_joined_block_size_rows", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxJoinedBlockSizeRows(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_joined_block_size_rows")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_insert_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxInsertThreads(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_insert_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_final_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxFinalThreads(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_final_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxThreads(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_alter_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxAlterThreads(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_alter_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_read_buffer_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxReadBufferSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_read_buffer_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_distributed_connections", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxDistributedConnections(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_distributed_connections")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_query_size", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxQuerySize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_query_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("interactive_delay", func(t *testing.T) {
		setting := NewSettings()
		setting.InteractiveDelay(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("interactive_delay")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("connect_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.ConnectTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("connect_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("connect_timeout_with_failover_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ConnectTimeoutWithFailoverMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("connect_timeout_with_failover_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("connect_timeout_with_failover_secure_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ConnectTimeoutWithFailoverSecureMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("connect_timeout_with_failover_secure_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("receive_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.ReceiveTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("receive_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("send_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.SendTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("send_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("tcp_keep_alive_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.TCPKeepAliveTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("tcp_keep_alive_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("queue_max_wait_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.QueueMaxWaitMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("queue_max_wait_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("connection_pool_max_wait_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ConnectionPoolMaxWaitMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("connection_pool_max_wait_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("replace_running_query_max_wait_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ReplaceRunningQueryMaxWaitMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("replace_running_query_max_wait_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("kafka_max_wait_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.KafkaMaxWaitMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("kafka_max_wait_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("rabbitmq_max_wait_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.RabbitmqMaxWaitMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("rabbitmq_max_wait_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("poll_interval", func(t *testing.T) {
		setting := NewSettings()
		setting.PollInterval(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("poll_interval")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("idle_connection_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.IdleConnectionTimeout(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("idle_connection_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_connections_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedConnectionsPoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_connections_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("connections_with_failover_max_tries", func(t *testing.T) {
		setting := NewSettings()
		setting.ConnectionsWithFailoverMaxTries(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("connections_with_failover_max_tries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("extremes", func(t *testing.T) {
		setting := NewSettings()
		setting.Extremes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("extremes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("use_uncompressed_cache", func(t *testing.T) {
		setting := NewSettings()
		setting.UseUncompressedCache(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("use_uncompressed_cache")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("replace_running_query", func(t *testing.T) {
		setting := NewSettings()
		setting.ReplaceRunningQuery(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("replace_running_query")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("background_buffer_flush_schedule_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.BackgroundBufferFlushSchedulePoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("background_buffer_flush_schedule_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("background_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.BackgroundPoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("background_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("background_move_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.BackgroundMovePoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("background_move_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("background_schedule_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.BackgroundSchedulePoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("background_schedule_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("background_distributed_schedule_pool_size", func(t *testing.T) {
		setting := NewSettings()
		setting.BackgroundDistributedSchedulePoolSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("background_distributed_schedule_pool_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_directory_monitor_sleep_time_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedDirectoryMonitorSleepTimeMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_directory_monitor_sleep_time_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_directory_monitor_max_sleep_time_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedDirectoryMonitorMaxSleepTimeMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_directory_monitor_max_sleep_time_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_directory_monitor_batch_inserts", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedDirectoryMonitorBatchInserts(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_directory_monitor_batch_inserts")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_move_to_prewhere", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeMoveToPrewhere(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_move_to_prewhere")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("replication_alter_partitions_sync", func(t *testing.T) {
		setting := NewSettings()
		setting.ReplicationAlterPartitionsSync(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("replication_alter_partitions_sync")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("replication_alter_columns_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.ReplicationAlterColumnsTimeout(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("replication_alter_columns_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_suspicious_low_cardinality_types", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowSuspiciousLowCardinalityTypes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_suspicious_low_cardinality_types")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("compile_expressions", func(t *testing.T) {
		setting := NewSettings()
		setting.CompileExpressions(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("compile_expressions")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_count_to_compile_expression", func(t *testing.T) {
		setting := NewSettings()
		setting.MinCountToCompileExpression(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_count_to_compile_expression")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("group_by_two_level_threshold", func(t *testing.T) {
		setting := NewSettings()
		setting.GroupByTwoLevelThreshold(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("group_by_two_level_threshold")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("group_by_two_level_threshold_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.GroupByTwoLevelThresholdBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("group_by_two_level_threshold_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_aggregation_memory_efficient", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedAggregationMemoryEfficient(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_aggregation_memory_efficient")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("aggregation_memory_efficient_merge_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.AggregationMemoryEfficientMergeThreads(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("aggregation_memory_efficient_merge_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_parallel_replicas", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxParallelReplicas(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_parallel_replicas")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("parallel_replicas_count", func(t *testing.T) {
		setting := NewSettings()
		setting.ParallelReplicasCount(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("parallel_replicas_count")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("parallel_replica_offset", func(t *testing.T) {
		setting := NewSettings()
		setting.ParallelReplicaOffset(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("parallel_replica_offset")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("skip_unavailable_shards", func(t *testing.T) {
		setting := NewSettings()
		setting.SkipUnavailableShards(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("skip_unavailable_shards")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_group_by_no_merge", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedGroupByNoMerge(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_group_by_no_merge")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("parallel_distributed_insert_select", func(t *testing.T) {
		setting := NewSettings()
		setting.ParallelDistributedInsertSelect(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("parallel_distributed_insert_select")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_distributed_group_by_sharding_key", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeDistributedGroupByShardingKey(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_distributed_group_by_sharding_key")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_skip_unused_shards", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeSkipUnusedShards(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_skip_unused_shards")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("force_optimize_skip_unused_shards", func(t *testing.T) {
		setting := NewSettings()
		setting.ForceOptimizeSkipUnusedShards(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("force_optimize_skip_unused_shards")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_skip_unused_shards_nesting", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeSkipUnusedShardsNesting(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_skip_unused_shards_nesting")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("force_optimize_skip_unused_shards_nesting", func(t *testing.T) {
		setting := NewSettings()
		setting.ForceOptimizeSkipUnusedShardsNesting(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("force_optimize_skip_unused_shards_nesting")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_parallel_parsing", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatParallelParsing(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_parallel_parsing")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("merge_tree_min_rows_for_seek", func(t *testing.T) {
		setting := NewSettings()
		setting.MergeTreeMinRowsForSeek(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("merge_tree_min_rows_for_seek")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("merge_tree_min_bytes_for_seek", func(t *testing.T) {
		setting := NewSettings()
		setting.MergeTreeMinBytesForSeek(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("merge_tree_min_bytes_for_seek")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("merge_tree_coarse_index_granularity", func(t *testing.T) {
		setting := NewSettings()
		setting.MergeTreeCoarseIndexGranularity(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("merge_tree_coarse_index_granularity")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("mysql_max_rows_to_insert", func(t *testing.T) {
		setting := NewSettings()
		setting.MysqlMaxRowsToInsert(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("mysql_max_rows_to_insert")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_min_equality_disjunction_chain_length", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeMinEqualityDisjunctionChainLength(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_min_equality_disjunction_chain_length")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_bytes_to_use_direct_io", func(t *testing.T) {
		setting := NewSettings()
		setting.MinBytesToUseDirectIo(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_bytes_to_use_direct_io")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_bytes_to_use_mmap_io", func(t *testing.T) {
		setting := NewSettings()
		setting.MinBytesToUseMmapIo(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_bytes_to_use_mmap_io")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("force_index_by_date", func(t *testing.T) {
		setting := NewSettings()
		setting.ForceIndexByDate(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("force_index_by_date")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("force_primary_key", func(t *testing.T) {
		setting := NewSettings()
		setting.ForcePrimaryKey(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("force_primary_key")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_streams_to_max_threads_ratio", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxStreamsToMaxThreadsRatio("1.11")
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_streams_to_max_threads_ratio")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1.11")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_streams_multiplier_for_merge_tables", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxStreamsMultiplierForMergeTables("1.11")
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_streams_multiplier_for_merge_tables")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1.11")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("network_zstd_compression_level", func(t *testing.T) {
		setting := NewSettings()
		setting.NetworkZstdCompressionLevel(4)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("network_zstd_compression_level")
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("priority", func(t *testing.T) {
		setting := NewSettings()
		setting.Priority(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("priority")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("os_thread_priority", func(t *testing.T) {
		setting := NewSettings()
		setting.OsThreadPriority(4)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("os_thread_priority")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("log_queries", func(t *testing.T) {
		setting := NewSettings()
		setting.LogQueries(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("log_queries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("log_queries_cut_to_length", func(t *testing.T) {
		setting := NewSettings()
		setting.LogQueriesCutToLength(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("log_queries_cut_to_length")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_concurrent_queries_for_user", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxConcurrentQueriesForUser(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_concurrent_queries_for_user")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_deduplicate", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertDeduplicate(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_deduplicate")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_quorum", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertQuorum(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_quorum")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_quorum_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertQuorumTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_quorum_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("select_sequential_consistency", func(t *testing.T) {
		setting := NewSettings()
		setting.SelectSequentialConsistency(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("select_sequential_consistency")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("table_function_remote_max_addresses", func(t *testing.T) {
		setting := NewSettings()
		setting.TableFunctionRemoteMaxAddresses(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("table_function_remote_max_addresses")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("read_backoff_min_latency_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ReadBackoffMinLatencyMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("read_backoff_min_latency_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("read_backoff_max_throughput", func(t *testing.T) {
		setting := NewSettings()
		setting.ReadBackoffMaxThroughput(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("read_backoff_max_throughput")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("read_backoff_min_interval_between_events_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.ReadBackoffMinIntervalBetweenEventsMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("read_backoff_min_interval_between_events_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("read_backoff_min_events", func(t *testing.T) {
		setting := NewSettings()
		setting.ReadBackoffMinEvents(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("read_backoff_min_events")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("use_client_time_zone", func(t *testing.T) {
		setting := NewSettings()
		setting.UseClientTimeZone(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("use_client_time_zone")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("fsync_metadata", func(t *testing.T) {
		setting := NewSettings()
		setting.FsyncMetadata(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("fsync_metadata")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("join_use_nulls", func(t *testing.T) {
		setting := NewSettings()
		setting.JoinUseNulls(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("join_use_nulls")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("any_join_distinct_right_table_keys", func(t *testing.T) {
		setting := NewSettings()
		setting.AnyJoinDistinctRightTableKeys(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("any_join_distinct_right_table_keys")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("preferred_block_size_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.PreferredBlockSizeBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("preferred_block_size_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_replica_delay_for_distributed_queries", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxReplicaDelayForDistributedQueries(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_replica_delay_for_distributed_queries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("fallback_to_stale_replicas_for_distributed_queries", func(t *testing.T) {
		setting := NewSettings()
		setting.FallbackToStaleReplicasForDistributedQueries(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("fallback_to_stale_replicas_for_distributed_queries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("preferred_max_column_in_block_size_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.PreferredMaxColumnInBlockSizeBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("preferred_max_column_in_block_size_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_distributed_sync", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertDistributedSync(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_distributed_sync")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_distributed_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertDistributedTimeout(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_distributed_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_ddl_task_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedDDLTaskTimeout(4)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_ddl_task_timeout")
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("stream_flush_interval_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.StreamFlushIntervalMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("stream_flush_interval_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("stream_poll_timeout_ms", func(t *testing.T) {
		setting := NewSettings()
		setting.StreamPollTimeoutMs(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("stream_poll_timeout_ms")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4000")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("insert_allow_materialized_columns", func(t *testing.T) {
		setting := NewSettings()
		setting.InsertAllowMaterializedColumns(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("insert_allow_materialized_columns")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_throw_if_noop", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeThrowIfNoop(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_throw_if_noop")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("use_index_for_in_with_subqueries", func(t *testing.T) {
		setting := NewSettings()
		setting.UseIndexForInWithSubqueries(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("use_index_for_in_with_subqueries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("joined_subquery_requires_alias", func(t *testing.T) {
		setting := NewSettings()
		setting.JoinedSubqueryRequiresAlias(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("joined_subquery_requires_alias")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("empty_result_for_aggregation_by_empty_set", func(t *testing.T) {
		setting := NewSettings()
		setting.EmptyResultForAggregationByEmptySet(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("empty_result_for_aggregation_by_empty_set")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_distributed_ddl", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowDistributedDDL(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_distributed_ddl")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_suspicious_codecs", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowSuspiciousCodecs(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_suspicious_codecs")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("odbc_max_field_size", func(t *testing.T) {
		setting := NewSettings()
		setting.OdbcMaxFieldSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("odbc_max_field_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("query_profiler_real_time_period_ns", func(t *testing.T) {
		setting := NewSettings()
		setting.QueryProfilerRealTimePeriodNs(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("query_profiler_real_time_period_ns")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("query_profiler_cpu_time_period_ns", func(t *testing.T) {
		setting := NewSettings()
		setting.QueryProfilerCPUTimePeriodNs(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("query_profiler_cpu_time_period_ns")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("metrics_perf_events_enabled", func(t *testing.T) {
		setting := NewSettings()
		setting.MetricsPerfEventsEnabled(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("metrics_perf_events_enabled")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_to_read", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsToRead(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_to_read")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_to_read", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesToRead(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_to_read")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_to_group_by", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsToGroupBy(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_to_group_by")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_before_external_group_by", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesBeforeExternalGroupBy(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_before_external_group_by")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_to_sort", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsToSort(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_to_sort")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_to_sort", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesToSort(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_to_sort")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_before_external_sort", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesBeforeExternalSort(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_before_external_sort")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_before_remerge_sort", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesBeforeRemergeSort(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_before_remerge_sort")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_result_rows", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxResultRows(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_result_rows")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_result_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxResultBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_result_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_execution_time", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxExecutionTime(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_execution_time")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_execution_speed", func(t *testing.T) {
		setting := NewSettings()
		setting.MinExecutionSpeed(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_execution_speed")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_execution_speed", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxExecutionSpeed(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_execution_speed")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_execution_speed_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.MinExecutionSpeedBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_execution_speed_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_execution_speed_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxExecutionSpeedBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_execution_speed_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("timeout_before_checking_execution_speed", func(t *testing.T) {
		setting := NewSettings()
		setting.TimeoutBeforeCheckingExecutionSpeed(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("timeout_before_checking_execution_speed")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_columns_to_read", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxColumnsToRead(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_columns_to_read")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_temporary_columns", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxTemporaryColumns(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_temporary_columns")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_temporary_non_const_columns", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxTemporaryNonConstColumns(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_temporary_non_const_columns")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_subquery_depth", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxSubqueryDepth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_subquery_depth")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_pipeline_depth", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxPipelineDepth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_pipeline_depth")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_ast_depth", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxAstDepth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_ast_depth")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_ast_elements", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxAstElements(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_ast_elements")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_expanded_ast_elements", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxExpandedAstElements(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_expanded_ast_elements")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("readonly", func(t *testing.T) {
		setting := NewSettings()
		setting.Readonly(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("readonly")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_in_set", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsInSet(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_in_set")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_in_set", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesInSet(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_in_set")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_in_join", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsInJoin(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_in_join")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_in_join", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesInJoin(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_in_join")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("join_any_take_last_row", func(t *testing.T) {
		setting := NewSettings()
		setting.JoinAnyTakeLastRow(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("join_any_take_last_row")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("partial_merge_join_optimizations", func(t *testing.T) {
		setting := NewSettings()
		setting.PartialMergeJoinOptimizations(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("partial_merge_join_optimizations")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("default_max_bytes_in_join", func(t *testing.T) {
		setting := NewSettings()
		setting.DefaultMaxBytesInJoin(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("default_max_bytes_in_join")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("partial_merge_join_left_table_buffer_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.PartialMergeJoinLeftTableBufferBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("partial_merge_join_left_table_buffer_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("partial_merge_join_rows_in_right_blocks", func(t *testing.T) {
		setting := NewSettings()
		setting.PartialMergeJoinRowsInRightBlocks(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("partial_merge_join_rows_in_right_blocks")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("join_on_disk_max_files_to_merge", func(t *testing.T) {
		setting := NewSettings()
		setting.JoinOnDiskMaxFilesToMerge(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("join_on_disk_max_files_to_merge")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_to_transfer", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsToTransfer(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_to_transfer")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_to_transfer", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesToTransfer(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_to_transfer")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_rows_in_distinct", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxRowsInDistinct(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_rows_in_distinct")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_bytes_in_distinct", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxBytesInDistinct(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_bytes_in_distinct")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_memory_usage", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxMemoryUsage(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_memory_usage")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_memory_usage_for_user", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxMemoryUsageForUser(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_memory_usage_for_user")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("memory_profiler_step", func(t *testing.T) {
		setting := NewSettings()
		setting.MemoryProfilerStep(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("memory_profiler_step")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_network_bandwidth", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxNetworkBandwidth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_network_bandwidth")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_network_bytes", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxNetworkBytes(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_network_bytes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_network_bandwidth_for_user", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxNetworkBandwidthForUser(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_network_bandwidth_for_user")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_network_bandwidth_for_all_users", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxNetworkBandwidthForAllUsers(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_network_bandwidth_for_all_users")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("log_profile_events", func(t *testing.T) {
		setting := NewSettings()
		setting.LogProfileEvents(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("log_profile_events")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("log_query_settings", func(t *testing.T) {
		setting := NewSettings()
		setting.LogQuerySettings(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("log_query_settings")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("log_query_threads", func(t *testing.T) {
		setting := NewSettings()
		setting.LogQueryThreads(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("log_query_threads")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_optimize_predicate_expression", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableOptimizePredicateExpression(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_optimize_predicate_expression")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_optimize_predicate_expression_to_final_subquery", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableOptimizePredicateExpressionToFinalSubquery(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_optimize_predicate_expression_to_final_subquery")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("low_cardinality_max_dictionary_size", func(t *testing.T) {
		setting := NewSettings()
		setting.LowCardinalityMaxDictionarySize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("low_cardinality_max_dictionary_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("low_cardinality_use_single_dictionary_for_part", func(t *testing.T) {
		setting := NewSettings()
		setting.LowCardinalityUseSingleDictionaryForPart(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("low_cardinality_use_single_dictionary_for_part")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("decimal_check_overflow", func(t *testing.T) {
		setting := NewSettings()
		setting.DecimalCheckOverflow(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("decimal_check_overflow")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("prefer_localhost_replica", func(t *testing.T) {
		setting := NewSettings()
		setting.PreferLocalhostReplica(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("prefer_localhost_replica")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_fetch_partition_retries_count", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxFetchPartitionRetriesCount(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_fetch_partition_retries_count")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("calculate_text_stack_trace", func(t *testing.T) {
		setting := NewSettings()
		setting.CalculateTextStackTrace(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("calculate_text_stack_trace")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_ddl", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowDDL(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_ddl")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("parallel_view_processing", func(t *testing.T) {
		setting := NewSettings()
		setting.ParallelViewProcessing(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("parallel_view_processing")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_debug_queries", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableDebugQueries(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_debug_queries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_unaligned_array_join", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableUnalignedArrayJoin(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_unaligned_array_join")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_read_in_order", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeReadInOrder(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_read_in_order")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_aggregation_in_order", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeAggregationInOrder(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_aggregation_in_order")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("read_in_order_two_level_merge_threshold", func(t *testing.T) {
		setting := NewSettings()
		setting.ReadInOrderTwoLevelMergeThreshold(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("read_in_order_two_level_merge_threshold")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("low_cardinality_allow_in_native_format", func(t *testing.T) {
		setting := NewSettings()
		setting.LowCardinalityAllowInNativeFormat(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("low_cardinality_allow_in_native_format")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("external_table_functions_use_nulls", func(t *testing.T) {
		setting := NewSettings()
		setting.ExternalTableFunctionsUseNulls(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("external_table_functions_use_nulls")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_hyperscan", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowHyperscan(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_hyperscan")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_simdjson", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowSimdjson(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_simdjson")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_introspection_functions", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowIntrospectionFunctions(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_introspection_functions")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_partitions_per_insert_block", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxPartitionsPerInsertBlock(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_partitions_per_insert_block")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("check_query_single_value_result", func(t *testing.T) {
		setting := NewSettings()
		setting.CheckQuerySingleValueResult(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("check_query_single_value_result")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_drop_detached", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowDropDetached(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_drop_detached")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_replica_error_half_life", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedReplicaErrorHalfLife(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_replica_error_half_life")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_replica_error_cap", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedReplicaErrorCap(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_replica_error_cap")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("distributed_replica_max_ignored_errors", func(t *testing.T) {
		setting := NewSettings()
		setting.DistributedReplicaMaxIgnoredErrors(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("distributed_replica_max_ignored_errors")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_live_view", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalLiveView(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_live_view")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("live_view_heartbeat_interval", func(t *testing.T) {
		setting := NewSettings()
		setting.LiveViewHeartbeatInterval(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("live_view_heartbeat_interval")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_live_view_insert_blocks_before_refresh", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxLiveViewInsertBlocksBeforeRefresh(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_live_view_insert_blocks_before_refresh")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_free_disk_space_for_temporary_data", func(t *testing.T) {
		setting := NewSettings()
		setting.MinFreeDiskSpaceForTemporaryData(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_free_disk_space_for_temporary_data")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_database_atomic", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalDatabaseAtomic(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_database_atomic")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("show_table_uuid_in_table_create_query_if_not_nil", func(t *testing.T) {
		setting := NewSettings()
		setting.ShowTableUUIDInTableCreateQueryIfNotNil(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("show_table_uuid_in_table_create_query_if_not_nil")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_scalar_subquery_optimization", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableScalarSubqueryOptimization(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_scalar_subquery_optimization")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_trivial_count_query", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeTrivialCountQuery(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_trivial_count_query")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("mutations_sync", func(t *testing.T) {
		setting := NewSettings()
		setting.MutationsSync(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("mutations_sync")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_move_functions_out_of_any", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeMoveFunctionsOutOfAny(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_move_functions_out_of_any")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_arithmetic_operations_in_aggregate_functions", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeArithmeticOperationsInAggregateFunctions(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_arithmetic_operations_in_aggregate_functions")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_duplicate_order_by_and_distinct", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeDuplicateOrderByAndDistinct(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_duplicate_order_by_and_distinct")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_if_chain_to_miltiif", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeIfChainToMiltiif(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_if_chain_to_miltiif")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_alter_materialized_view_structure", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalAlterMaterializedViewStructure(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_alter_materialized_view_structure")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("enable_early_constant_folding", func(t *testing.T) {
		setting := NewSettings()
		setting.EnableEarlyConstantFolding(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("enable_early_constant_folding")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("deduplicate_blocks_in_dependent_materialized_views", func(t *testing.T) {
		setting := NewSettings()
		setting.DeduplicateBlocksInDependentMaterializedViews(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("deduplicate_blocks_in_dependent_materialized_views")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("use_compact_format_in_distributed_parts_names", func(t *testing.T) {
		setting := NewSettings()
		setting.UseCompactFormatInDistributedPartsNames(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("use_compact_format_in_distributed_parts_names")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("multiple_joins_rewriter_version", func(t *testing.T) {
		setting := NewSettings()
		setting.MultipleJoinsRewriterVersion(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("multiple_joins_rewriter_version")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("validate_polygons", func(t *testing.T) {
		setting := NewSettings()
		setting.ValidatePolygons(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("validate_polygons")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_parser_depth", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxParserDepth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_parser_depth")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("temporary_live_view_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.TemporaryLiveViewTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("temporary_live_view_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("transform_null_in", func(t *testing.T) {
		setting := NewSettings()
		setting.TransformNullIn(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("transform_null_in")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_nondeterministic_mutations", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowNondeterministicMutations(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_nondeterministic_mutations")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("lock_acquire_timeout", func(t *testing.T) {
		setting := NewSettings()
		setting.LockAcquireTimeout(durExample)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("lock_acquire_timeout")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("4")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("materialize_ttl_after_modify", func(t *testing.T) {
		setting := NewSettings()
		setting.MaterializeTTLAfterModify(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("materialize_ttl_after_modify")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_geo_types", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalGeoTypes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_geo_types")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("data_type_default_nullable", func(t *testing.T) {
		setting := NewSettings()
		setting.DataTypeDefaultNullable(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("data_type_default_nullable")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("cast_keep_nullable", func(t *testing.T) {
		setting := NewSettings()
		setting.CastKeepNullable(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("cast_keep_nullable")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_low_cardinality_type", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalLowCardinalityType(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_low_cardinality_type")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("compile", func(t *testing.T) {
		setting := NewSettings()
		setting.Compile(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("compile")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("min_count_to_compile", func(t *testing.T) {
		setting := NewSettings()
		setting.MinCountToCompile(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("min_count_to_compile")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_multiple_joins_emulation", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalMultipleJoinsEmulation(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_multiple_joins_emulation")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_cross_to_join_conversion", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalCrossToJoinConversion(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_cross_to_join_conversion")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("allow_experimental_data_skipping_indices", func(t *testing.T) {
		setting := NewSettings()
		setting.AllowExperimentalDataSkippingIndices(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("allow_experimental_data_skipping_indices")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("merge_tree_uniform_read_distribution", func(t *testing.T) {
		setting := NewSettings()
		setting.MergeTreeUniformReadDistribution(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("merge_tree_uniform_read_distribution")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("mark_cache_min_lifetime", func(t *testing.T) {
		setting := NewSettings()
		setting.MarkCacheMinLifetime(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("mark_cache_min_lifetime")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("partial_merge_join", func(t *testing.T) {
		setting := NewSettings()
		setting.PartialMergeJoin(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("partial_merge_join")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("max_memory_usage_for_all_queries", func(t *testing.T) {
		setting := NewSettings()
		setting.MaxMemoryUsageForAllQueries(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("max_memory_usage_for_all_queries")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("force_optimize_skip_unused_shards_no_nested", func(t *testing.T) {
		setting := NewSettings()
		setting.ForceOptimizeSkipUnusedShardsNoNested(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("force_optimize_skip_unused_shards_no_nested")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("experimental_use_processors", func(t *testing.T) {
		setting := NewSettings()
		setting.ExperimentalUseProcessors(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("experimental_use_processors")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("format_csv_delimiter", func(t *testing.T) {
		setting := NewSettings()
		setting.FormatCsvDelimiter(',')
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("format_csv_delimiter")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String(",")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("format_csv_allow_single_quotes", func(t *testing.T) {
		setting := NewSettings()
		setting.FormatCsvAllowSingleQuotes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("format_csv_allow_single_quotes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("format_csv_allow_double_quotes", func(t *testing.T) {
		setting := NewSettings()
		setting.FormatCsvAllowDoubleQuotes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("format_csv_allow_double_quotes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_csv_crlf_end_of_line", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatCsvCrlfEndOfLine(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_csv_crlf_end_of_line")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_csv_unquoted_null_literal_as_null", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatCsvUnquotedNullLiteralAsNull(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_csv_unquoted_null_literal_as_null")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_skip_unknown_fields", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatSkipUnknownFields(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_skip_unknown_fields")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_with_names_use_header", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatWithNamesUseHeader(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_with_names_use_header")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_import_nested_json", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatImportNestedJSON(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_import_nested_json")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_aggregators_of_group_by_keys", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeAggregatorsOfGroupByKeys(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_aggregators_of_group_by_keys")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_defaults_for_omitted_fields", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatDefaultsForOmittedFields(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_defaults_for_omitted_fields")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_tsv_empty_as_default", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatTsvEmptyAsDefault(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_tsv_empty_as_default")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_null_as_default", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatNullAsDefault(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_null_as_default")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("optimize_group_by_function_keys", func(t *testing.T) {
		setting := NewSettings()
		setting.OptimizeGroupByFunctionKeys(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("optimize_group_by_function_keys")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_values_interpret_expressions", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatValuesInterpretExpressions(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_values_interpret_expressions")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_values_deduce_templates_of_expressions", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatValuesDeduceTemplatesOfExpressions(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_values_deduce_templates_of_expressions")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_values_accurate_types_of_literals", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatValuesAccurateTypesOfLiterals(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_values_accurate_types_of_literals")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_avro_allow_missing_fields", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatAvroAllowMissingFields(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_avro_allow_missing_fields")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_json_quote_64bit_integers", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatJSONQuote64bitIntegers(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_json_quote_64bit_integers")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_json_quote_denormals", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatJSONQuoteDenormals(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_json_quote_denormals")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_json_escape_forward_slashes", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatJSONEscapeForwardSlashes(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_json_escape_forward_slashes")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_pretty_max_rows", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatPrettyMaxRows(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_pretty_max_rows")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_pretty_max_column_pad_width", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatPrettyMaxColumnPadWidth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_pretty_max_column_pad_width")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_pretty_max_value_width", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatPrettyMaxValueWidth(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_pretty_max_value_width")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_pretty_color", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatPrettyColor(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_pretty_color")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_parquet_row_group_size", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatParquetRowGroupSize(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_parquet_row_group_size")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_tsv_crlf_end_of_line", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatTsvCrlfEndOfLine(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_tsv_crlf_end_of_line")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_allow_errors_num", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatAllowErrorsNum(2)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_allow_errors_num")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("2")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("input_format_allow_errors_ratio", func(t *testing.T) {
		setting := NewSettings()
		setting.InputFormatAllowErrorsRatio("1.11")
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("input_format_allow_errors_ratio")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1.11")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("format_regexp_skip_unmatched", func(t *testing.T) {
		setting := NewSettings()
		setting.FormatRegexpSkipUnmatched(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("format_regexp_skip_unmatched")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_enable_streaming", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatEnableStreaming(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_enable_streaming")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})

	t.Run("output_format_write_statistics", func(t *testing.T) {
		setting := NewSettings()
		setting.OutputFormatWriteStatistics(true)
		writerExcept := NewWriter()
		writerActual := NewWriter()
		writerExcept.String("output_format_write_statistics")
		// flag
		writerExcept.Uint8(0)
		writerExcept.String("1")
		setting.WriteTo(writerActual.output)
		require.Equal(t, writerExcept.output.Bytes(), writerActual.output.Bytes())
	})
}
