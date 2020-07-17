package chconn

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/vahid-sohrabloo/chconn/internal/ctxwatch"
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
	dbmsMinRevisionWithClientInfo           = 54032
	dbmsMinRevisionWithServerTimezone       = 54058
	dbmsMinRevisionWithQuotaKeyInClientInfo = 54060
	dbmsMinRevisionWithServerDisplayName    = 54372
	dbmsMinRevisionWithVersionPatch         = 54401
	dbmsMinRevisionWithClientWriteInfo      = 54420
)

const (
	dbmsVersionMajor    = 1
	dbmsVersionMinor    = 0
	dbmsVersionPatch    = 0
	dbmsVersionRevision = 54420
)

const (
	// Need to read additional keys.
	// Additional keys are stored before indexes as value N and N keys
	// after them.
	hasAdditionalKeysBit = 1 << 9
	// Need to update dictionary.
	// It means that previous granule has different dictionary.
	needUpdateDictionary = 1 << 10

	serializationType = hasAdditionalKeysBit | needUpdateDictionary
)

type QueryProcessingStage uint64

const (

	// QueryProcessingStageComplete Completely.
	QueryProcessingStageComplete QueryProcessingStage = 2
)

// DialFunc is a function that can be used to connect to a ClickHouse server.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// LookupFunc is a function that can be used to lookup IPs addrs from host.
type LookupFunc func(ctx context.Context, host string) (addrs []string, err error)

// ReaderFunc is a function that can be used get reader for read from server
type ReaderFunc func(io.Reader) io.Reader

// WriterFunc is a function that can be used get writer to writer from server
// Note: DO NOT Use bufio.Wriert, chconn do not support flush
type WriterFunc func(io.Writer) io.Writer

// Conn is a low-level Clickhoue connection handle. It is not safe for concurrent usage.
type Conn interface {
	RawConn() net.Conn
	Close(ctx context.Context) error
	IsClosed() bool
	IsBusy() bool
	ServerInfo() ServerInfo
	Ping(ctx context.Context) error
	Exec(ctx context.Context, query string) (interface{}, error)
	ExecWithSetting(ctx context.Context, query string, setting *Settings) (interface{}, error)
	ExecCallback(ctx context.Context, query string, setting *Settings, onProgress func(*Progress)) (interface{}, error)
	Insert(ctx context.Context, query string) (InsertStmt, error)
	InsertWithSetting(ctx context.Context, query string, setting *Settings) (InsertStmt, error)
	Select(ctx context.Context, query string) (SelectStmt, error)
	SelectWithSetting(ctx context.Context, query string, setting *Settings) (SelectStmt, error)
	SelectCallback(
		ctx context.Context,
		query string,
		setting *Settings,
		onProgress func(*Progress),
		onProfile func(*Profile)) (SelectStmt, error)
}
type conn struct {
	conn              net.Conn          // the underlying TCP connection
	parameterStatuses map[string]string // parameters that have been reported by the server
	serverInfo        ServerInfo
	clientInfo        *ClientInfo

	config *Config

	status byte // One of connStatus* constants

	writer   *Writer
	writerto io.Writer
	reader   *Reader

	contextWatcher *ctxwatch.ContextWatcher
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
		return nil, &connectError{config: config, msg: "hostname resolving error", err: errors.New("ip addr wasn't found")}
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
	c.writer = NewWriter()
	if config.ReaderFunc != nil {
		c.reader = NewReader(config.ReaderFunc(c.conn))
	} else {
		c.reader = NewReader(bufio.NewReader(c.conn))
	}
	if config.WriterFunc != nil {
		c.writerto = config.WriterFunc(c.conn)
	} else {
		c.writerto = c.conn
	}

	c.serverInfo = ServerInfo{
		Timezone: time.Local,
	}
	err = c.hello()
	if err != nil {
		return nil, err
	}
	c.status = connStatusIdle
	return c, nil
}

func (ch *conn) RawConn() net.Conn {
	return ch.conn
}
func (ch *conn) hello() error {
	ch.writer.Uvarint(clientHello)
	ch.writer.String(ch.config.ClientName)
	ch.writer.Uvarint(dbmsVersionMajor)
	ch.writer.Uvarint(dbmsVersionMinor)
	ch.writer.Uvarint(dbmsVersionRevision)
	ch.writer.String(ch.config.Database)
	ch.writer.String(ch.config.User)
	ch.writer.String(ch.config.Password)

	if _, err := ch.writer.WriteTo(ch.writerto); err != nil {
		return errors.Errorf("write hello: %w", err)
	}

	res, err := ch.reciveAndProccessData(emptyOnProgress)
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
	ctx context.Context, //nolint:unparam //maybe use later
	query,
	queryID string, //nolint:unparam //maybe use later
	setting *Settings,
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
	if setting != nil {
		//nolint:errcheck // no need for bytes.Buffer
		setting.WriteTo(ch.writer.output)
	}

	ch.writer.String("")

	ch.writer.Uvarint(uint64(QueryProcessingStageComplete))

	// comprestion
	ch.writer.Uvarint(0)

	ch.writer.String(query)

	return ch.sendData(newBlock(setting))
}

func (ch *conn) sendData(block *block) error {
	ch.writer.Uvarint(clientData)
	// name
	ch.writer.String("")
	return block.write(ch)
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
func (ch *conn) reciveAndProccessData(onProgress func(*Progress)) (interface{}, error) {
	packet, err := ch.reader.Uvarint()
	if err != nil {
		return nil, &readError{"packet: read packet type", err}
	}
	switch packet {
	case serverData, serverTotals, serverExtremes:
		block := newBlock(nil)
		err = block.read(ch)
		return block, err
	case serverProfileInfo:
		profile := newProfile()

		err = profile.read(ch)
		return profile, err
	case serverProgress:
		progress := newProgress()
		err = progress.read(ch)
		if err == nil && onProgress != nil {
			onProgress(progress)
			return ch.reciveAndProccessData(onProgress)
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

		return ch.reciveAndProccessData(onProgress)
	}
	return nil, &notImplementedPacket{packet: packet}
}

var emptyOnProgress = func(*Progress) {

}

func (ch *conn) Exec(ctx context.Context, query string) (interface{}, error) {
	return ch.ExecCallback(ctx, query, nil, nil)
}

func (ch *conn) ExecWithSetting(ctx context.Context, query string, setting *Settings) (interface{}, error) {
	return ch.ExecCallback(ctx, query, setting, nil)
}

func (ch *conn) ExecCallback(
	ctx context.Context,
	query string,
	setting *Settings,
	onProgress func(*Progress),
) (interface{}, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}
	defer ch.unlock()

	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()

	err = ch.sendQueryWithOption(ctx, query, "", setting)
	if err != nil {
		return nil, err
	}
	if onProgress == nil {
		onProgress = emptyOnProgress
	}

	return ch.reciveAndProccessData(onProgress)
}

// Insert send query for insert and prepare insert stmt
func (ch *conn) Insert(ctx context.Context, query string) (InsertStmt, error) {
	return ch.InsertWithSetting(ctx, query, nil)
}

// Insert send query for insert and prepare insert stmt with setting option
func (ch *conn) InsertWithSetting(ctx context.Context, query string, setting *Settings) (InsertStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}
	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()

	err = ch.sendQueryWithOption(ctx, query, "", setting)
	if err != nil {
		return nil, err
	}
	res, err := ch.reciveAndProccessData(emptyOnProgress)

	if err != nil {
		return nil, err
	}
	block, ok := res.(*block)
	if !ok {
		return nil, &unexpectedPacket{expected: "serverData", actual: res}
	}
	block.setting = setting

	err = block.initForInsert(ch)
	if err != nil {
		return nil, err
	}
	return &insertStmt{
		block:      block,
		conn:       ch,
		query:      query,
		queryID:    "",
		stage:      QueryProcessingStageComplete,
		settings:   setting,
		clientInfo: nil,
	}, nil
}

// Select send query for select and prepare SelectStmt
func (ch *conn) Select(ctx context.Context, query string) (SelectStmt, error) {
	return ch.SelectCallback(ctx, query, nil, nil, nil)
}

// Select send query for select and prepare SelectStmt with settion option
func (ch *conn) SelectWithSetting(ctx context.Context, query string, setting *Settings) (SelectStmt, error) {
	return ch.SelectCallback(ctx, query, setting, nil, nil)
}

// Select send query for select and prepare SelectStmt on register  on progress and on profile callback
func (ch *conn) SelectCallback(
	ctx context.Context,
	query string,
	setting *Settings,
	onProgress func(*Progress),
	onProfile func(*Profile),
) (SelectStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}

	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()

	err = ch.sendQueryWithOption(ctx, query, "", setting)
	if err != nil {
		return nil, err
	}
	return &selectStmt{
		conn:       ch,
		query:      query,
		onProgress: onProgress,
		onProfile:  onProfile,
		queryID:    "",
		setting:    setting,
		clientInfo: nil,
	}, nil
}
