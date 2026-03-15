package chconn

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/ctxwatch"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

const (
	connStatusUninitialized = iota
	connStatusConnecting
	connStatusClosed
	connStatusIdle
	connStatusBusy
)

const (
	// Name, version, revision, default DB
	clientHello = 0
	// whether the compression must be used,
	// query text (without data for INSERTs).
	clientQuery = 1
	// A block of data (compressed or not).
	clientData = 2
	// Check that connection to the server is alive.
	clientPing = 4
)

const (
	// Name, version, revision.
	serverHello = 0
	// A block of data (compressed or not).
	serverData = 1
	// The exception during query execution.
	serverException = 2
	// Query execution progress: rows read, bytes read.
	serverProgress = 3
	// Ping response
	serverPong = 4
	// All packets were transmitted
	serverEndOfStream = 5
	// Packet with profiling info.
	serverProfileInfo = 6
	// A block with totals (compressed or not).
	serverTotals = 7
	// A block with minimums and maximums (compressed or not).
	serverExtremes = 8
	// Columns' description for default values calculation
	serverTableColumns = 11
	// list of unique parts ids.
	//nolint:deadcode,unused,varcheck
	serverPartUUIDs = 12
	// String (UUID) describes a request for which next task is needed
	//nolint:deadcode,unused,varcheck
	serverReadTaskRequest = 13
	// Packet with profile events from server
	serverProfileEvents = 14
	// Receive server's (session-wide) default timezone
	serverTimezoneUpdate = 17
)

const (
	clientVersionMajor = 1
	clientVersionMinor = 0
	clientVersionPatch = 0
)

type queryProcessingStage uint64

const (

	// queryProcessingStageComplete Completely.
	queryProcessingStageComplete queryProcessingStage = 2
)

// DialFunc is a function that can be used to connect to a ClickHouse server.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// LookupFunc is a function that can be used to lookup IPs addrs from host.
type LookupFunc func(ctx context.Context, host string) (addrs []string, err error)

// ReaderFunc is a function that can be used get reader for read from server
type ReaderFunc func(io.Reader, Conn) io.Reader

// WriterFunc is a function that can be used to get writer to writer from server
// Note: DO NOT use bufio.Writer, chconn doesn't support flush
type WriterFunc func(io.Writer) io.Writer

// Conn is a low-level Clickhouse connection handle. It is not safe for concurrent usage.
type Conn interface {
	// RawConn Get Raw Connection. Do not use unless you know what you want to do
	RawConn() net.Conn
	// Close the connection to database
	Close() error
	// IsClosed reports if the connection has been closed.
	IsClosed() bool
	// IsBusy reports if the connection is busy.
	IsBusy() bool
	// ServerInfo get Server info
	ServerInfo() *shared.ServerInfo
	// Ping sends a ping to check that the connection to the server is alive.
	Ping(ctx context.Context) error
	// Exec executes a query without returning any rows.
	// NOTE: don't use it for insert and select query
	Exec(ctx context.Context, query string) error
	// ExecWithOption executes a query without returning any rows with Query options.
	// NOTE: don't use it for insert and select query
	ExecWithOption(
		ctx context.Context,
		query string,
		queryOptions *QueryOptions,
	) error
	// Insert executes a insert query and commit all columns data.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	Insert(ctx context.Context, query string, columns ...column.ColumnCore) error
	// InsertWithOption executes a insert query with the query options and commit all columns data.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	InsertWithOption(ctx context.Context, query string, queryOptions *QueryOptions, columns ...column.ColumnCore) error
	// Insert executes a insert query and return a InsertStmt.
	//
	// NOTE: only use for insert query
	InsertStream(ctx context.Context, query string) (InsertStmt, error)
	// InsertWithOption executes a insert query with the query options and return a InsertStmt.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	InsertStreamWithOption(
		ctx context.Context,
		query string,
		queryOptions *QueryOptions) (InsertStmt, error)
	// Select executes a query and return select stmt.
	//
	// NOTE: only use for select query
	Select(ctx context.Context, query string, columns ...column.ColumnCore) (SelectStmt, error)
	// Select executes a query with the the query options and return select stmt.
	//
	// NOTE: only use for select query
	SelectWithOption(
		ctx context.Context,
		query string,
		queryOptions *QueryOptions,
		columns ...column.ColumnCore,
	) (SelectStmt, error)

	// Query sends a select query to the server and returns a Rows to read the results. Only errors encountered sending the query
	// and initializing Rows will be returned. Err() on the returned Rows must be checked after the Rows is closed to
	// determine if the query executed successfully.
	//
	// For better performance use Select instead of Query when possible. specially when you want to read al lot of data.
	//
	// The returned Rows must be closed before the connection can be used again. It is safe to attempt to read from the
	// returned Rows even if an error is returned. The error will be the available in rows.Err() after rows are closed. It
	// is allowed to ignore the error returned from Query and handle it in Rows.
	//
	// It is possible for a query to return one or more rows before encountering an error. In most cases the rows should be
	// collected before processing rather than processed while receiving each row. This avoids the possibility of the
	// application processing rows from a query that the server rejected. The CollectRows function is useful here.
	//
	// NOTE: Only use this function for select queries (or any other queries that return rows).
	Query(ctx context.Context, sql string, args ...Parameter) (Rows, error)

	// QueryWithOption is the same as Query but with QueryOptions
	QueryWithOption(ctx context.Context, sql string, queryOption *QueryOptions, args ...Parameter) (Rows, error)

	// QueryRow is a convenience wrapper over Query. Any error that occurs while
	// querying is deferred until calling Scan on the returned Row. That Row will
	// error with ErrNoRows if no rows are returned.
	QueryRow(ctx context.Context, sql string, args ...Parameter) Row

	// QueryRowWithOptions is the same as QueryRow but with QueryOptions
	QueryRowWithOption(ctx context.Context, sql string, queryOption *QueryOptions, args ...Parameter) Row
}
type writeFlusher interface {
	io.Writer
	Flush() error
}

type conn struct {
	conn              net.Conn          // the underlying TCP connection
	parameterStatuses map[string]string // parameters that have been reported by the server
	serverInfo        *shared.ServerInfo
	clientInfo        *ClientInfo

	config *Config

	status byte // One of connStatus* constants

	writer           *readerwriter.Writer
	writerTo         io.Writer
	writerToCompress io.Writer

	reader   *readerwriter.Reader
	compress bool

	contextWatcher *ctxwatch.ContextWatcher
	block          *block

	profileEvent *ProfileEvent
}

// Connect establishes a connection to a ClickHouse server using the environment and connString (in URL or DSN format)
// to provide configuration. See documentation for ParseConfig for details. ctx can be used to cancel a connect attempt.
func Connect(ctx context.Context, connString string) (Conn, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// ConnectConfig establishes a connection to a ClickHouse server using config. config must have been constructed with
// ParseConfig. ctx can be used to cancel a connect attempt.
//
// If config.Fallbacks are present they will sequentially be tried in case of error establishing network connection. An
// authentication error will terminate the chain of attempts (like libpq:
// https://www.postgresql.org/docs/12/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS) and be returned as the error. Otherwise,
// if all attempts fail the last error is returned.
func ConnectConfig(octx context.Context, config *Config) (c Conn, err error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	// Simplify usage by treating primary config and fallbacks the same.
	fallbackConfigs := []*FallbackConfig{
		{
			Host:      config.Host,
			Port:      config.Port,
			TLSConfig: config.TLSConfig,
		},
	}
	fallbackConfigs = append(fallbackConfigs, config.Fallbacks...)
	ctx := octx
	var lookupErrors []error
	fallbackConfigs, lookupErrors = expandWithIPs(ctx, config.LookupFunc, fallbackConfigs)
	if len(fallbackConfigs) == 0 {
		// If no hosts resolved, report the first lookup error if available.
		if len(lookupErrors) > 0 {
			return nil, &connectError{config: config, msg: "hostname resolving error", err: lookupErrors[0]}
		}
		return nil, &connectError{config: config, msg: "hostname resolving error", err: ErrIPNotFound}
	}

	foundBestServer := false
	var fallbackConfig *FallbackConfig
	for _, fc := range fallbackConfigs {
		// ConnectTimeout restricts the whole connection process.
		if config.ConnectTimeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(octx, config.ConnectTimeout)
			//nolint:gocritic
			defer cancel()
		} else {
			ctx = octx
		}
		c, err = connect(ctx, config, fc)
		if err == nil {
			foundBestServer = true
			break
		} else if chErr, ok := err.(*ChError); ok {
			return nil, &connectError{config: config, msg: "server error", err: chErr}
		}
	}

	if !foundBestServer && fallbackConfig != nil {
		c, err = connect(ctx, config, fallbackConfig)
		if cherr, ok := err.(*ChError); ok {
			err = &connectError{config: config, msg: "server error", err: cherr}
		}
	}

	if err != nil {
		return nil, err // no need to wrap in connectError because it will already be wrapped in all cases except ChError
	}

	if config.AfterConnect != nil {
		err := config.AfterConnect(ctx, c)
		if err != nil {
			c.RawConn().Close()
			return nil, &connectError{config: config, msg: "AfterConnect error", err: err}
		}
	}

	return c, nil
}

func expandWithIPs(ctx context.Context, lookupFn LookupFunc, fallbacks []*FallbackConfig) ([]*FallbackConfig, []error) {
	var configs []*FallbackConfig
	var errs []error

	for _, fb := range fallbacks {
		ips, err := lookupFn(ctx, fb.Host)
		if err != nil {
			// Skip hosts that fail to resolve instead of aborting all fallbacks.
			// Collect errors so the caller can report them if no hosts resolve.
			errs = append(errs, err)
			continue
		}

		for _, ip := range ips {
			splitIP, splitPort, err := net.SplitHostPort(ip)
			if err == nil {
				port, err := strconv.ParseUint(splitPort, 10, 16)
				if err != nil {
					errs = append(errs, fmt.Errorf("error parsing port (%s) from lookup: %w", splitPort, err))
					continue
				}
				configs = append(configs, &FallbackConfig{
					Host:      splitIP,
					Port:      uint16(port),
					TLSConfig: fb.TLSConfig,
				})
			} else {
				configs = append(configs, &FallbackConfig{
					Host:      ip,
					Port:      fb.Port,
					TLSConfig: fb.TLSConfig,
				})
			}
		}
	}

	return configs, errs
}

func connect(ctx context.Context, config *Config, fallbackConfig *FallbackConfig) (Conn, error) {
	c := new(conn)
	c.config = config

	c.compress = config.Compress != CompressNone

	var err error
	network, address := NetworkAddress(fallbackConfig.Host, fallbackConfig.Port)
	c.conn, err = config.DialFunc(ctx, network, address)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			err = &errTimeout{err: err}
		}
		return nil, &connectError{config: config, msg: "dial error", err: err}
	}

	c.parameterStatuses = make(map[string]string)

	if fallbackConfig.TLSConfig != nil {
		c.conn = tls.Client(c.conn, fallbackConfig.TLSConfig)
	}

	c.status = connStatusConnecting
	c.contextWatcher = ctxwatch.NewContextWatcher(
		func() {
			c.conn.SetDeadline(time.Date(1, 1, 1, 1, 1, 1, 1, time.UTC)) //nolint:errcheck //no need
		},
		func() {
			c.conn.SetDeadline(time.Time{}) //nolint:errcheck //no need
		},
	)

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return nil, newContextAlreadyDoneError(ctx)
		default:
		}
		c.contextWatcher.Watch(ctx)
		defer c.contextWatcher.Unwatch()
	}

	c.writer = readerwriter.NewWriter()
	if config.ReaderFunc != nil {
		c.reader = readerwriter.NewReader(config.ReaderFunc(c.conn, c))
	} else {
		c.reader = readerwriter.NewReader(bufio.NewReaderSize(c.conn, c.config.MinReadBufferSize))
	}
	if config.WriterFunc != nil {
		c.writerTo = config.WriterFunc(c.conn)
	} else {
		c.writerTo = c.conn
	}
	if c.compress {
		c.writerToCompress = readerwriter.NewCompressWriter(c.writerTo, byte(config.Compress))
	} else {
		c.writerToCompress = c.writerTo
	}

	c.serverInfo = &shared.ServerInfo{}
	err = c.hello()
	if err != nil {
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}

	c.sendAddendum()

	c.block = newBlock(c)
	c.profileEvent = newProfileEvent()
	c.status = connStatusIdle

	return c, nil
}

func (ch *conn) sendAddendum() {
	v := ch.negotiatedVersion()
	if v >= helper.DbmsMinProtocolWithQuotaKey {
		ch.writer.String(ch.config.QuotaKey)
	}
	if v >= helper.DbmsMinProtocolVersionWithChunkedPackets {
		ch.writer.String("notchunked") // proto_send_chunked
		ch.writer.String("notchunked") // proto_recv_chunked
	}
	if v >= helper.DbmsMinRevisionWithVersionedParallelReplicas {
		ch.writer.Uvarint(0) // parallel replicas protocol version
	}
}

func (ch *conn) flushCompress() error {
	if w, ok := ch.writerToCompress.(writeFlusher); ok {
		return w.Flush()
	}
	return nil
}

func (ch *conn) RawConn() net.Conn {
	return ch.conn
}

// send hello to ClickHouse
func (ch *conn) hello() error {
	ch.writer.Uvarint(clientHello)
	ch.writer.String(ch.config.ClientName)
	ch.writer.Uvarint(clientVersionMajor)
	ch.writer.Uvarint(clientVersionMinor)
	ch.writer.Uvarint(helper.ClientTCPVersion)
	ch.writer.String(ch.config.Database)
	ch.writer.String(ch.config.User)
	ch.writer.String(ch.config.Password)

	if _, err := ch.writer.WriteTo(ch.writerTo); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	res, err := ch.receiveAndProcessData(emptyQueryOptions)
	if err != nil {
		return err
	}
	if ch.serverInfo.Revision == 0 {
		return &unexpectedPacket{expected: "serverHello", actual: res}
	}
	return nil
}

// IsClosed reports if the connection has been closed.
func (ch *conn) IsClosed() bool {
	return ch.status < connStatusIdle
}

// IsBusy reports if the connection is busy.
func (ch *conn) IsBusy() bool {
	return ch.status == connStatusBusy
}

// lock locks the connection.
func (ch *conn) lock() error {
	switch ch.status {
	case connStatusBusy:
		return &connLockError{status: "conn busy"} // This only should be possible in case of an application bug.
	case connStatusClosed:
		return &connLockError{status: "conn closed"}
	case connStatusUninitialized:
		return &connLockError{status: "conn uninitialized"}
	}
	ch.status = connStatusBusy
	return nil
}

func (ch *conn) unlock() {
	switch ch.status {
	case connStatusBusy:
		ch.status = connStatusIdle
	case connStatusClosed:
	default:
		panic("BUG: cannot unlock unlocked connection") // This should only be possible if there is a bug in this package.
	}
}

func (ch *conn) sendQueryWithOption(
	query,
	queryID string,
	settings Settings,
	parameters *Parameters,
) error {
	ch.writer.Uvarint(clientQuery)
	ch.writer.String(queryID)
	if ch.serverInfo.Revision >= helper.DbmsMinRevisionWithClientInfo {
		if ch.clientInfo == nil {
			ch.clientInfo = &ClientInfo{}
		}

		ch.clientInfo.fillOSUserHostNameAndVersionInfo()
		ch.clientInfo.ClientName = ch.config.Database + " " + ch.config.ClientName

		ch.clientInfo.write(ch)
	}

	// setting
	if settings != nil && ch.serverInfo.Revision >= helper.DbmsMinRevisionWithSettingsSerializedAsStrings {
		settings.write(ch.writer)
	}

	ch.writer.String("")

	if ch.negotiatedVersion() >= helper.DbmsMinProtocolWithInterserverExternallyGrantedRoles {
		// externally granted roles — not used by external clients
		ch.writer.String("")
	}

	if ch.serverInfo.Revision >= helper.DbmsMinRevisionWithInterServerSecret {
		ch.writer.String("")
	}

	ch.writer.Uvarint(uint64(queryProcessingStageComplete))

	// compression
	if ch.compress {
		ch.writer.Uint8(1)
	} else {
		ch.writer.Uint8(0)
	}

	ch.writer.String(query)

	if ch.serverInfo.Revision >= helper.DbmsMinProtocolWithParameters {
		parameters.write(ch.writer)
		ch.writer.String("")
	} else if parameters.hasParam() {
		return errors.New("parameters are not supported by the server")
	}

	return ch.sendEmptyBlock()
}

func (ch *conn) sendData(block *block, numRows int) error {
	ch.writer.Uvarint(clientData)
	// name
	ch.writer.String("")

	// if compress enable we must send this part with uncompressed data
	if ch.compress {
		_, err := ch.writer.WriteTo(ch.writerTo)
		if err != nil {
			return &writeError{"write block info", err}
		}
	}
	return block.writeHeader(numRows)
}

func (ch *conn) sendEmptyBlock() error {
	ch.block.reset()
	return ch.sendData(ch.block, 0)
}

func (ch *conn) Close() error {
	if ch.status == connStatusClosed {
		return nil
	}
	ch.contextWatcher.Unwatch()
	ch.status = connStatusClosed
	return ch.conn.Close()
}

func (ch *conn) readTableColumn() {
	// todo check errors
	ch.reader.String() //nolint:errcheck //no needed
	ch.reader.String() //nolint:errcheck //no needed
}
func (ch *conn) handleServerException() error {
	chErr := &ChError{}
	if errRead := chErr.read(ch.reader); errRead != nil {
		ch.Close()
		return errRead
	}
	// Close connection by default, unless OnError callback says otherwise.
	if ch.config.OnError == nil || ch.config.OnError(ch, chErr) {
		ch.Close()
	}
	return chErr
}

func (ch *conn) receiveAndProcessData(queryOption *QueryOptions) (any, error) {
	packet, err := ch.reader.Uvarint()
	if err != nil {
		return nil, &readError{"packet: read packet type", err}
	}
	switch packet {
	case serverData, serverTotals, serverExtremes:
		ch.block.reset()
		err = ch.block.read()
		return ch.block, err
	case serverProfileInfo:
		profile := newProfile()

		err = profile.read(ch)
		if err != nil {
			return nil, err
		}
		if queryOption.OnProfile != nil {
			queryOption.OnProfile(profile)
		}
		return ch.receiveAndProcessData(queryOption)
	case serverProgress:
		progress := newProgress()
		err = progress.read(ch)
		if err != nil {
			return nil, err
		}
		if queryOption.OnProgress != nil {
			queryOption.OnProgress(progress)
		}
		return ch.receiveAndProcessData(queryOption)

	case serverHello:
		err = readServerInfo(ch.serverInfo, ch.reader)
		return nil, err
	case serverPong:
		return &pong{}, err
	case serverException:
		return nil, ch.handleServerException()
	case serverEndOfStream:
		return nil, nil

	case serverTableColumns:
		// At version >= 54481, table columns are sent compressed when compression is enabled.
		if ch.negotiatedVersion() >= helper.DbmsMinRevisionWithCompressedLogsProfileEvents && ch.compress {
			ch.reader.SetCompress(true)
		}
		ch.readTableColumn()
		ch.reader.SetCompress(false)
		return ch.receiveAndProcessData(queryOption)
	case serverProfileEvents:
		ch.block.reset()
		oldCompress := ch.compress
		// Profile events are compressed starting from 54481; before that always uncompressed.
		if ch.negotiatedVersion() < helper.DbmsMinRevisionWithCompressedLogsProfileEvents {
			ch.compress = false
		}
		err = ch.block.read()
		if err != nil {
			ch.compress = oldCompress
			return nil, err
		}
		err := ch.profileEvent.read(ch)
		ch.compress = oldCompress
		if err != nil {
			return nil, err
		}
		if queryOption.OnProfileEvent != nil {
			queryOption.OnProfileEvent(ch.profileEvent)
		}
		return ch.receiveAndProcessData(queryOption)
	case serverTimezoneUpdate:
		// TODO: save timezone
		ch.reader.String() //nolint:errcheck //no needed
		return ch.receiveAndProcessData(queryOption)
	}

	// serverLog (10) — system logs of query execution; skip the block like profile events.
	if packet == 10 {
		ch.block.reset()
		oldCompress := ch.compress
		if ch.negotiatedVersion() < helper.DbmsMinRevisionWithCompressedLogsProfileEvents {
			ch.compress = false
		}
		err = ch.block.read()
		ch.compress = oldCompress
		if err != nil {
			return nil, err
		}
		return ch.receiveAndProcessData(queryOption)
	}

	return nil, &notImplementedPacket{packet: packet}
}

var emptyQueryOptions = &QueryOptions{}

type QueryOptions struct {
	QueryID        string
	Settings       Settings
	OnProgress     func(*Progress)
	OnProfile      func(*Profile)
	OnProfileEvent func(*ProfileEvent)
	Parameters     *Parameters
}

func (ch *conn) Exec(ctx context.Context, query string) error {
	return ch.ExecWithOption(ctx, query, nil)
}

func (ch *conn) ExecWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions,
) error {
	stmt, err := ch.SelectWithOption(ctx, query, queryOptions)
	if err != nil {
		return err
	}
	if stmt != nil {
		for stmt.Next() {
		}
		return stmt.Err()
	}
	return nil
}

func readServerInfo(srv *shared.ServerInfo, r *readerwriter.Reader) error {
	// The server decides which fields to include based on the client's TCP version,
	// so we must use the minimum of client and server versions when determining
	// which fields to read.
	return readServerInfoWithClientVersion(srv, r, helper.ClientTCPVersion)
}

func readServerInfoWithClientVersion(srv *shared.ServerInfo, r *readerwriter.Reader, clientVersion uint64) (err error) {
	if srv.Name, err = r.String(); err != nil {
		return &readError{"ServerInfo: could not read server name", err}
	}
	if srv.MajorVersion, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server major version", err}
	}
	if srv.MinorVersion, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server minor version", err}
	}
	if srv.Revision, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server revision", err}
	}

	// The server decides which fields to include based on the CLIENT's TCP version
	// (what we sent in hello), not its own revision. Use min(client, server) to
	// determine which fields the server actually sent.
	v := clientVersion
	if srv.Revision < v {
		v = srv.Revision
	}

	// Fields are in the exact order the server's sendHello writes them.
	if v >= helper.DbmsMinRevisionWithVersionedParallelReplicas {
		if _, err = r.Uvarint(); err != nil {
			return &readError{"ServerInfo: could not read parallel replicas protocol version", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithServerTimezone {
		if srv.Timezone, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read server timezone", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithServerDisplayName {
		if srv.ServerDisplayName, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read server display name", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithVersionPatch {
		if srv.ServerVersionPatch, err = r.Uvarint(); err != nil {
			return &readError{"ServerInfo: could not read server version patch", err}
		}
	}
	if v >= helper.DbmsMinProtocolVersionWithChunkedPackets {
		if _, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read proto_send_chunked_srv", err}
		}
		if _, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read proto_recv_chunked_srv", err}
		}
	}
	return readServerInfoExtended(srv, r, v)
}

func readServerInfoExtended(srv *shared.ServerInfo, r *readerwriter.Reader, v uint64) (err error) {
	if v >= helper.DbmsMinProtocolVersionWithPasswordComplexityRules {
		lenRules, err := r.Uvarint()
		if err != nil {
			return &readError{"ServerInfo: could not read server password complexity rules: len", err}
		}
		srv.PasswordPatterns = make([]shared.ServerInfoPasswordRules, lenRules)
		for i := uint64(0); i < lenRules; i++ {
			var rule shared.ServerInfoPasswordRules
			if rule.Pattern, err = r.String(); err != nil {
				return &readError{"ServerInfo: could not read server password complexity rules: pattern", err}
			}
			if rule.Message, err = r.String(); err != nil {
				return &readError{"ServerInfo: could not read server password complexity rules: pattern", err}
			}
			srv.PasswordPatterns[i] = rule
		}
	}
	if v >= helper.DbmsMinRevisionWithInterserverSecretV2 {
		if _, err = r.Uint64(); err != nil {
			return &readError{"ServerInfo: could not read server interserver secret nonce", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithServerSettings {
		if err = skipSettings(r); err != nil {
			return &readError{"ServerInfo: could not read server settings", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithQueryPlanSerialization {
		if _, err = r.Uvarint(); err != nil {
			return &readError{"ServerInfo: could not read query plan serialization version", err}
		}
	}
	if v >= helper.DbmsMinRevisionWithVersionedClusterFunction {
		if _, err = r.Uvarint(); err != nil {
			return &readError{"ServerInfo: could not read cluster function protocol version", err}
		}
	}
	return nil
}

// negotiatedVersion returns the effective protocol version for this connection.
// The server sends fields based on the client's advertised version, so we must
// use min(client, server) to know which fields are actually present on the wire.
func (ch *conn) negotiatedVersion() uint64 {
	v := uint64(helper.ClientTCPVersion)
	if ch.serverInfo.Revision < v {
		v = ch.serverInfo.Revision
	}
	return v
}

// ServerInfo get server info
func (ch *conn) ServerInfo() *shared.ServerInfo {
	return ch.serverInfo
}

func NewWriter() *readerwriter.Writer {
	return readerwriter.NewWriter()
}

func NewReader(r io.Reader) *readerwriter.Reader {
	return readerwriter.NewReader(r)
}
