package chconn

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/internal/ctxwatch"
	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/setting"
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
)

const (
	dbmsMinRevisionWithClientInfo                  = 54032
	dbmsMinRevisionWithServerTimezone              = 54058
	dbmsMinRevisionWithQuotaKeyInClientInfo        = 54060
	dbmsMinRevisionWithServerDisplayName           = 54372
	dbmsMinRevisionWithVersionPatch                = 54401
	dbmsMinRevisionWithClientWriteInfo             = 54420
	dbmsMinRevisionWithSettingsSerializedAsStrings = 54429
	dbmsMinRevisionWithInterserverSecret           = 54441
	dbmsMinRevisionWithOpentelemetry               = 54442
)

const (
	dbmsVersionMajor    = 1
	dbmsVersionMinor    = 0
	dbmsVersionPatch    = 0
	dbmsVersionRevision = 54442
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
type ReaderFunc func(io.Reader) io.Reader

// WriterFunc is a function that can be used to get writer to writer from server
// Note: DO NOT use bufio.Writer, chconn doesn't support flush
type WriterFunc func(io.Writer) io.Writer

// Conn is a low-level Clickhouse connection handle. It is not safe for concurrent usage.
type Conn interface {
	// RawConn Get Raw Connection. Do not use unless you know what you want to do
	RawConn() net.Conn
	// Close the connection to database
	Close(ctx context.Context) error
	// IsClosed reports if the connection has been closed.
	IsClosed() bool
	// IsBusy reports if the connection is busy.
	IsBusy() bool
	// ServerInfo get Server info
	ServerInfo() ServerInfo
	// Ping sends a ping to check that the connection to the server is alive.
	Ping(ctx context.Context) error
	// Exec executes a query without returning any rows.
	// NOTE: don't use it for insert and select query
	Exec(ctx context.Context, query string) (interface{}, error)
	// ExecWithSetting executes a query without returning any rows with the setting option.
	// NOTE: don't use it for insert and select query
	ExecWithSetting(ctx context.Context, query string, settings *setting.Settings) (interface{}, error)
	// ExecCallback executes a query without returning any rows with the setting option and on progress callback.
	// NOTE: don't use it for insert and select query
	ExecCallback(
		ctx context.Context,
		query string,
		settings *setting.Settings,
		queryID string,
		onProgress func(*Progress),
	) (interface{}, error)
	// Insert executes a query and commit all columns data.
	// NOTE: only use for insert query
	Insert(ctx context.Context, query string, columns ...column.Column) error
	// InsertWithSetting executes a query with the setting option and commit all columns data.
	// NOTE: only use for insert query
	InsertWithSetting(ctx context.Context, query string, settings *setting.Settings, queryID string, columns ...column.Column) error
	// Select executes a query and return select stmt.
	// NOTE: only use for select query
	Select(ctx context.Context, query string) (SelectStmt, error)
	// Select executes a query with the setting option and return select stmt.
	// NOTE: only use for select query
	SelectWithSetting(ctx context.Context, query string, settings *setting.Settings) (SelectStmt, error)
	// Select executes a query with the setting option, on progress callback, on profile callback and return select stmt.
	// NOTE: only use for select query
	SelectCallback(
		ctx context.Context,
		query string,
		settings *setting.Settings,
		queryID string,
		onProgress func(*Progress),
		onProfile func(*Profile)) (SelectStmt, error)
}

type writeFlusher interface {
	io.Writer
	Flush() error
}

type conn struct {
	conn              net.Conn          // the underlying TCP connection
	parameterStatuses map[string]string // parameters that have been reported by the server
	serverInfo        ServerInfo
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
}

// Connect establishes a connection to a ClickHouse server using the environment and connString (in URL or DSN format)
// to provide configuration. See documention for ParseConfig for details. ctx can be used to cancel a connect attempt.
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
func ConnectConfig(ctx context.Context, config *Config) (c Conn, err error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	if config.ConnectTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.ConnectTimeout)
		defer cancel()
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

	fallbackConfigs, err = expandWithIPs(ctx, config.LookupFunc, fallbackConfigs)
	if err != nil {
		return nil, &connectError{config: config, msg: "hostname resolving error", err: err}
	}

	if len(fallbackConfigs) == 0 {
		return nil, &connectError{config: config, msg: "hostname resolving error", err: ErrIPNotFound}
	}

	for _, fc := range fallbackConfigs {
		c, err = connect(ctx, config, fc)
		if err == nil {
			break
		} else if err, ok := err.(*ChError); ok {
			return nil, &connectError{config: config, msg: "server error", err: err}
		}
	}

	if err != nil {
		return nil, err // no need to wrap in connectError because it will already be wrapped in all cases except ChError
	}

	if config.AfterConnect != nil {
		err := config.AfterConnect(ctx, c)
		if err != nil {
			//nolint:errcheck
			c.RawConn().Close()
			return nil, &connectError{config: config, msg: "AfterConnect error", err: err}
		}
	}

	return c, nil
}

func expandWithIPs(ctx context.Context, lookupFn LookupFunc, fallbacks []*FallbackConfig) ([]*FallbackConfig, error) {
	var configs []*FallbackConfig

	for _, fb := range fallbacks {
		ips, err := lookupFn(ctx, fb.Host)
		if err != nil {
			return nil, err
		}

		for _, ip := range ips {
			configs = append(configs, &FallbackConfig{
				Host:      ip,
				Port:      fb.Port,
				TLSConfig: fb.TLSConfig,
			})
		}
	}

	return configs, nil
}

func connect(ctx context.Context, config *Config, fallbackConfig *FallbackConfig) (Conn, error) {
	c := new(conn)
	c.config = config

	c.compress = config.Compress

	var err error
	network, address := NetworkAddress(fallbackConfig.Host, fallbackConfig.Port)
	c.conn, err = config.DialFunc(ctx, network, address)
	if err != nil {
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

	c.contextWatcher.Watch(ctx)
	defer c.contextWatcher.Unwatch()
	c.writer = readerwriter.NewWriter()
	if config.ReaderFunc != nil {
		c.reader = readerwriter.NewReader(config.ReaderFunc(c.conn))
	} else {
		c.reader = readerwriter.NewReader(bufio.NewReaderSize(c.conn, 4096))
	}
	if config.WriterFunc != nil {
		c.writerTo = config.WriterFunc(c.conn)
	} else {
		c.writerTo = c.conn
	}
	if c.compress {
		c.writerToCompress = readerwriter.NewCompressWriter(c.writerTo)
	} else {
		c.writerToCompress = c.writerTo
	}

	c.serverInfo = ServerInfo{
		Timezone: time.Local,
	}
	err = c.hello()
	if err != nil {
		return nil, err
	}
	c.block = newBlock()
	c.status = connStatusIdle

	return c, nil
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
	ch.writer.Uvarint(dbmsVersionMajor)
	ch.writer.Uvarint(dbmsVersionMinor)
	ch.writer.Uvarint(dbmsVersionRevision)
	ch.writer.String(ch.config.Database)
	ch.writer.String(ch.config.User)
	ch.writer.String(ch.config.Password)

	if _, err := ch.writer.WriteTo(ch.writerTo); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	res, err := ch.receiveAndProccessData(emptyOnProgress)
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
	ctx context.Context, //nolint:unparam
	query,
	queryID string,
	settings *setting.Settings,
) error {
	ch.writer.Uvarint(clientQuery)
	ch.writer.String(queryID)
	if ch.serverInfo.Revision >= dbmsMinRevisionWithClientInfo {
		if ch.clientInfo == nil {
			ch.clientInfo = &ClientInfo{}
		}

		ch.clientInfo.fillOSUserHostNameAndVersionInfo()
		ch.clientInfo.ClientName = ch.config.Database + " " + ch.config.ClientName

		ch.clientInfo.write(ch)
	}

	// setting
	if settings != nil {
		//nolint:errcheck // no need for bytes.Buffer
		settings.WriteTo(ch.writer.Output(),
			ch.serverInfo.Revision >= dbmsMinRevisionWithSettingsSerializedAsStrings)
	}

	ch.writer.String("")

	if ch.serverInfo.Revision >= dbmsMinRevisionWithInterserverSecret {
		ch.writer.String("")
	}

	ch.writer.Uvarint(uint64(queryProcessingStageComplete))

	// compression
	if ch.compress {
		ch.writer.Uvarint(1)
	} else {
		ch.writer.Uvarint(0)
	}

	ch.writer.String(query)
	return ch.sendEmptyBlock()
}

func (ch *conn) sendData(block *block, numRows int) error {
	ch.writer.Uvarint(clientData)
	// name
	ch.writer.String("")

	// if compress enable we must send to this part with uncompressed data
	if ch.compress {
		_, err := ch.writer.WriteTo(ch.writerTo)
		if err != nil {
			return &writeError{"block: write block info", err}
		}
	}
	return block.writeHeader(ch, numRows)
}

func (ch *conn) sendEmptyBlock() error {
	ch.block.reset()
	return ch.sendData(ch.block, 0)
}

func (ch *conn) Close(ctx context.Context) error {
	if ch.status == connStatusClosed {
		return nil
	}
	ch.status = connStatusClosed

	if ctx != context.Background() {
		ch.contextWatcher.Watch(ctx)
		defer ch.contextWatcher.Unwatch()
	}
	return ch.conn.Close()
}

func (ch *conn) readTableColumn() {
	ch.reader.String() //nolint:errcheck //no needed
	ch.reader.String() //nolint:errcheck //no needed
}
func (ch *conn) receiveAndProccessData(onProgress func(*Progress)) (interface{}, error) {
	packet, err := ch.reader.Uvarint()
	if err != nil {
		return nil, &readError{"packet: read packet type", err}
	}
	switch packet {
	case serverData, serverTotals, serverExtremes:
		ch.block.reset()
		err = ch.block.read(ch)
		return ch.block, err
	case serverProfileInfo:
		profile := newProfile()

		err = profile.read(ch)
		return profile, err
	case serverProgress:
		progress := newProgress()
		err = progress.read(ch)
		if err == nil && onProgress != nil {
			onProgress(progress)
			return ch.receiveAndProccessData(onProgress)
		}
		return progress, err
	case serverHello:
		err = ch.serverInfo.read(ch.reader)
		return nil, err
	case serverPong:
		return &pong{}, err
	case serverException:
		err := &ChError{}
		defer ch.Close(context.Background())
		if errRead := err.read(ch.reader); errRead != nil {
			return nil, errRead
		}
		return nil, err
	case serverEndOfStream:
		return nil, nil

	case serverTableColumns:
		ch.readTableColumn()

		return ch.receiveAndProccessData(onProgress)
	}
	return nil, &notImplementedPacket{packet: packet}
}

var emptyOnProgress = func(*Progress) {

}

func (ch *conn) Exec(ctx context.Context, query string) (interface{}, error) {
	return ch.ExecCallback(ctx, query, nil, "", nil)
}

func (ch *conn) ExecWithSetting(ctx context.Context, query string, settings *setting.Settings) (interface{}, error) {
	return ch.ExecCallback(ctx, query, settings, "", nil)
}

func (ch *conn) ExecCallback(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*Progress),
) (interface{}, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}
	defer ch.unlock()

	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()
	var hasError bool
	defer func() {
		if hasError {
			ch.Close(context.Background())
		}
	}()

	err = ch.sendQueryWithOption(ctx, query, queryID, settings)
	if err != nil {
		hasError = true
		return nil, err
	}
	if onProgress == nil {
		onProgress = emptyOnProgress
	}
	if err != nil {
		hasError = true
		return nil, err
	}
	res, err := ch.receiveAndProccessData(onProgress)
	if err != nil {
		hasError = true
		return nil, err
	}
	return res, nil
}

// Select send query for select and prepare SelectStmt
func (ch *conn) Select(ctx context.Context, query string) (SelectStmt, error) {
	return ch.SelectCallback(ctx, query, nil, "", nil, nil)
}

// Select send query for select and prepare SelectStmt with settion option
func (ch *conn) SelectWithSetting(ctx context.Context, query string, settings *setting.Settings) (SelectStmt, error) {
	return ch.SelectCallback(ctx, query, settings, "", nil, nil)
}

// Select send query for select and prepare SelectStmt on progress and on profile callback
func (ch *conn) SelectCallback(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*Progress),
	onProfile func(*Profile),
) (SelectStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}

	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()
	var hasError bool
	defer func() {
		if hasError {
			ch.Close(context.Background())
		}
	}()
	err = ch.sendQueryWithOption(ctx, query, queryID, settings)
	if err != nil {
		hasError = true
		return nil, err
	}
	return &selectStmt{
		conn:       ch,
		query:      query,
		onProgress: onProgress,
		onProfile:  onProfile,
		queryID:    queryID,
		clientInfo: nil,
	}, nil
}
