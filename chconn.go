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

	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/internal/ctxwatch"
	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
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
)

const (
	dbmsVersionMajor    = 1
	dbmsVersionMinor    = 0
	dbmsVersionPatch    = 0
	dbmsVersionRevision = 54460
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
	Close() error
	// IsClosed reports if the connection has been closed.
	IsClosed() bool
	// IsBusy reports if the connection is busy.
	IsBusy() bool
	// ServerInfo get Server info
	ServerInfo() *ServerInfo
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
	// Insert executes a query and commit all columns data.
	// NOTE: only use for insert query
	Insert(ctx context.Context, query string, columns ...column.ColumnBasic) error
	// InsertWithSetting executes a query with the query options and commit all columns data.
	// NOTE: only use for insert query
	InsertWithOption(ctx context.Context, query string, queryOptions *QueryOptions, columns ...column.ColumnBasic) error
	// Select executes a query and return select stmt.
	// NOTE: only use for select query
	Select(ctx context.Context, query string, columns ...column.ColumnBasic) (SelectStmt, error)
	// Select executes a query with the the query options and return select stmt.
	// NOTE: only use for select query
	SelectWithOption(
		ctx context.Context,
		query string,
		queryOptions *QueryOptions,
		columns ...column.ColumnBasic,
	) (SelectStmt, error)
}

type writeFlusher interface {
	io.Writer
	Flush() error
}

type conn struct {
	conn              net.Conn          // the underlying TCP connection
	parameterStatuses map[string]string // parameters that have been reported by the server
	serverInfo        *ServerInfo
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
	fallbackConfigs, err = expandWithIPs(ctx, config.LookupFunc, fallbackConfigs)
	if err != nil {
		return nil, &connectError{config: config, msg: "hostname resolving error", err: err}
	}

	if len(fallbackConfigs) == 0 {
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

func expandWithIPs(ctx context.Context, lookupFn LookupFunc, fallbacks []*FallbackConfig) ([]*FallbackConfig, error) {
	var configs []*FallbackConfig

	for _, fb := range fallbacks {
		ips, err := lookupFn(ctx, fb.Host)
		if err != nil {
			return nil, err
		}

		for _, ip := range ips {
			splitIP, splitPort, err := net.SplitHostPort(ip)
			if err == nil {
				port, err := strconv.ParseUint(splitPort, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("error parsing port (%s) from lookup: %w", splitPort, err)
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

	return configs, nil
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
		c.reader = readerwriter.NewReader(config.ReaderFunc(c.conn))
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

	c.serverInfo = &ServerInfo{}
	err = c.hello()
	if err != nil {
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}

	c.sendAddendum()

	c.block = newBlock()
	c.profileEvent = newProfileEvent()
	c.status = connStatusIdle

	return c, nil
}

func (ch *conn) sendAddendum() {
	if ch.serverInfo.Revision >= helper.DbmsMinProtocolWithQuotaKey {
		ch.writer.String(ch.config.QuotaKey)
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
	ch.writer.Uvarint(dbmsVersionMajor)
	ch.writer.Uvarint(dbmsVersionMinor)
	ch.writer.Uvarint(dbmsVersionRevision)
	ch.writer.String(ch.config.Database)
	ch.writer.String(ch.config.User)
	ch.writer.String(ch.config.Password)

	if _, err := ch.writer.WriteTo(ch.writerTo); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	res, err := ch.receiveAndProcessData(emptyOnProgress)
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
	return block.writeHeader(ch, numRows)
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
func (ch *conn) receiveAndProcessData(onProgress func(*Progress)) (interface{}, error) {
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
			return ch.receiveAndProcessData(onProgress)
		}
		return progress, err
	case serverHello:
		err = ch.serverInfo.read(ch.reader)
		return nil, err
	case serverPong:
		return &pong{}, err
	case serverException:
		err := &ChError{}
		defer ch.Close()
		if errRead := err.read(ch.reader); errRead != nil {
			return nil, errRead
		}
		return nil, err
	case serverEndOfStream:
		return nil, nil

	case serverTableColumns:
		ch.readTableColumn()
		return ch.receiveAndProcessData(onProgress)
	case serverProfileEvents:
		ch.block.reset()
		oldCompress := ch.compress
		defer func() {
			ch.compress = oldCompress
		}()
		ch.compress = false
		err = ch.block.read(ch)
		if err != nil {
			return nil, err
		}
		err := ch.profileEvent.read(ch)
		if err != nil {
			return nil, err
		}
		return ch.profileEvent, nil
	}
	return nil, &notImplementedPacket{packet: packet}
}

var emptyOnProgress = func(*Progress) {

}

var emptyQueryOptions = &QueryOptions{
	OnProgress: emptyOnProgress,
}

type QueryOptions struct {
	QueryID        string
	Settings       Settings
	OnProgress     func(*Progress)
	OnProfile      func(*Profile)
	OnProfileEvent func(*ProfileEvent)
	Parameters     *Parameters
	UseGoTime      bool
}

func (ch *conn) Exec(ctx context.Context, query string) error {
	return ch.ExecWithOption(ctx, query, nil)
}

func (ch *conn) ExecWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions,
) error {
	err := ch.lock()
	if err != nil {
		return err
	}
	defer func() {
		ch.unlock()
		if err != nil {
			ch.Close()
		}
	}()

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return newContextAlreadyDoneError(ctx)
		default:
		}
		ch.contextWatcher.Watch(ctx)
		defer ch.contextWatcher.Unwatch()
	}

	if queryOptions == nil {
		queryOptions = emptyQueryOptions
	}

	err = ch.sendQueryWithOption(query, queryOptions.QueryID, queryOptions.Settings, queryOptions.Parameters)
	if err != nil {
		return preferContextOverNetTimeoutError(ctx, err)
	}
	if queryOptions.OnProgress == nil {
		queryOptions.OnProgress = emptyOnProgress
	}

	_, err = ch.receiveAndProcessData(queryOptions.OnProgress)
	return preferContextOverNetTimeoutError(ctx, err)
}
