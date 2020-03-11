package chconn

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/vahid-sohrabloo/chconn/internal/ctxwatch"
	errors "golang.org/x/xerrors"
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
	// Cancel the query execution.
	clientCancel = 3
	// Check that connection to the server is alive.
	clientPing = 4
	// Check status of tables on the server.
	clientTablesStatusRequest = 5
	// Keep the connection alive
	clientKeepAlive = 6
	// A block of data (compressed or not).
	clientScalar = 7
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
	// A response to TablesStatus request.
	serverTablesStatusResponse = 9
	// System logs of the query execution
	serverLog = 10
	// Columns' description for default values calculation
	serverTableColumns = 11
)

const (
	DBMS_MIN_REVISION_WITH_CLIENT_INFO                               = 54032
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE                           = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO                  = 54060
	DBMS_MIN_REVISION_WITH_TABLES_STATUS                             = 54226
	DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE = 54337
	DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME                       = 54372
	DBMS_MIN_REVISION_WITH_VERSION_PATCH                             = 54401
	DBMS_MIN_REVISION_WITH_SERVER_LOGS                               = 54406
	DBMS_MIN_REVISION_WITH_CLIENT_SUPPORT_EMBEDDED_DATA              = 54415
	DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO                         = 54420
)

const (
	DBMS_VERSION_MAJOR    = 1
	DBMS_VERSION_MINOR    = 0
	DBMS_VERSION_PATCH    = 0
	DBMS_VERSION_REVISION = 54420
)

type QueryProcessingStage uint64

const (

	// Only read/have been read the columns specified in the query.
	QueryProcessingStageFetchColumns QueryProcessingStage = 0
	// Until the stage where the results of processing on different servers can be combined.
	QueryProcessingStageWithMergeableState QueryProcessingStage = 1
	// Completely.
	QueryProcessingStageComplete QueryProcessingStage = 2
)

// DialFunc is a function that can be used to connect to a PostgreSQL server.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// LookupFunc is a function that can be used to lookup IPs addrs from host.
type LookupFunc func(ctx context.Context, host string) (addrs []string, err error)

// Conn is a low-level Clickhoue connection handle. It is not safe for concurrent usage.
type Conn struct {
	conn              net.Conn          // the underlying TCP or unix domain socket connection
	secretKey         uint32            // key to use to send a cancel query message to the server
	parameterStatuses map[string]string // parameters that have been reported by the server
	txStatus          byte
	ServerInfo        ServerInfo

	config *Config

	status byte // One of connStatus* constants

	writer *Writer
	reader *Reader

	contextWatcher *ctxwatch.ContextWatcher
}

// Connect establishes a connection to a PostgreSQL server using the environment and connString (in URL or DSN format)
// to provide configuration. See documention for ParseConfig for details. ctx can be used to cancel a connect attempt.
func Connect(ctx context.Context, connString string) (*Conn, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// Connect establishes a connection to a PostgreSQL server using config. config must have been constructed with
// ParseConfig. ctx can be used to cancel a connect attempt.
//
// If config.Fallbacks are present they will sequentially be tried in case of error establishing network connection. An
// authentication error will terminate the chain of attempts (like libpq:
// https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS) and be returned as the error. Otherwise,
// if all attempts fail the last error is returned.
func ConnectConfig(ctx context.Context, config *Config) (conn *Conn, err error) {
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

	fallbackConfigs, err = expandWithIPs(ctx, config.LookupFunc, fallbackConfigs)
	if err != nil {
		return nil, &connectError{config: config, msg: "hostname resolving error", err: err}
	}

	if len(fallbackConfigs) == 0 {
		return nil, &connectError{config: config, msg: "hostname resolving error", err: errors.New("ip addr wasn't found")}
	}

	for _, fc := range fallbackConfigs {
		conn, err = connect(ctx, config, fc)
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
		err := config.AfterConnect(ctx, conn)
		if err != nil {
			conn.conn.Close()
			return nil, &connectError{config: config, msg: "AfterConnect error", err: err}
		}
	}

	return conn, nil
}

func expandWithIPs(ctx context.Context, lookupFn LookupFunc, fallbacks []*FallbackConfig) ([]*FallbackConfig, error) {
	var configs []*FallbackConfig

	for _, fb := range fallbacks {
		// skip resolve for unix sockets
		if strings.HasPrefix(fb.Host, "/") {
			configs = append(configs, &FallbackConfig{
				Host:      fb.Host,
				Port:      fb.Port,
				TLSConfig: fb.TLSConfig,
			})

			continue
		}

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

func connect(ctx context.Context, config *Config, fallbackConfig *FallbackConfig) (*Conn, error) {
	conn := new(Conn)
	conn.config = config

	var err error
	network, address := NetworkAddress(fallbackConfig.Host, fallbackConfig.Port)
	conn.conn, err = config.DialFunc(ctx, network, address)
	if err != nil {
		return nil, &connectError{config: config, msg: "dial error", err: err}
	}

	conn.parameterStatuses = make(map[string]string)

	if fallbackConfig.TLSConfig != nil {
		conn.conn = tls.Client(conn.conn, fallbackConfig.TLSConfig)
	}

	conn.status = connStatusConnecting
	conn.contextWatcher = ctxwatch.NewContextWatcher(
		// todo
		func() {},
		func() {},
	)

	conn.contextWatcher.Watch(ctx)
	defer conn.contextWatcher.Unwatch()
	conn.writer = NewWriter()
	conn.reader = NewReader(bufio.NewReader(conn.conn))
	conn.ServerInfo = ServerInfo{
		Timezone: time.Local,
	}
	err = conn.hello()
	if err != nil {
		return nil, &connectError{config: config, msg: "hello error", err: err}
	}
	conn.status = connStatusIdle
	return conn, nil
}

func (ch *Conn) RawConn() net.Conn {
	return ch.conn
}
func (ch *Conn) hello() error {

	ch.writer.Uvarint(clientHello)
	ch.writer.String(ch.config.ClientName)
	ch.writer.Uvarint(DBMS_VERSION_MAJOR)
	ch.writer.Uvarint(DBMS_VERSION_MINOR)
	ch.writer.Uvarint(DBMS_VERSION_REVISION)
	ch.writer.String(ch.config.Database)
	ch.writer.String(ch.config.User)
	ch.writer.String(ch.config.Password)

	if _, err := ch.writer.WriteTo(ch.conn); err != nil {
		return err
	}
	packet, err := ch.reader.Uvarint()

	if err != nil {
		return err
	}

	switch packet {
	case serverException:
		err := &ChError{}
		defer ch.conn.Close()
		if errRead := err.read(ch.reader); errRead != nil {
			return errRead
		}
		return err
	case serverHello:
		if err := ch.ServerInfo.Read(ch.reader); err != nil {
			return err
		}
	case serverEndOfStream:
		return nil
	default:
		ch.conn.Close()
		return &unexpectedPacket{expected: serverHello, actual: packet}
	}

	return nil

}

// IsClosed reports if the connection has been closed.
func (ch *Conn) IsClosed() bool {
	return ch.status < connStatusIdle
}

// IsBusy reports if the connection is busy.
func (ch *Conn) IsBusy() bool {
	return ch.status == connStatusBusy
}

// lock locks the connection.
func (ch *Conn) lock() error {
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

func (ch *Conn) unlock() {
	switch ch.status {
	case connStatusBusy:
		ch.status = connStatusIdle
	case connStatusClosed:
	default:
		panic("BUG: cannot unlock unlocked connection") // This should only be possible if there is a bug in this package.
	}
}

func (ch *Conn) SendQuery(ctx context.Context, query string) error {
	return ch.SendQueryWithOption(ctx, query, "", QueryProcessingStageComplete, nil, nil)
}

func (ch *Conn) Exec(ctx context.Context, query string) (interface{}, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}
	defer ch.unlock()
	err = ch.SendQueryWithOption(ctx, query, "", QueryProcessingStageComplete, nil, nil)
	if err != nil {
		return nil, err
	}
	return ch.ReciveAndProccessData()
}

func (ch *Conn) SendQueryWithOption(
	ctx context.Context,
	query string,
	queryID string,
	stage QueryProcessingStage,
	settings *Setting,
	clientInfo *ClientInfo) error {
	ch.writer.Uvarint(clientQuery)
	ch.writer.String(queryID)
	if ch.ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {

		if clientInfo == nil {
			clientInfo = &ClientInfo{}
		}
		if clientInfo.IsEmpty() {
			clientInfo.QueryKind = QureyKindInitialQuery
			clientInfo.fillOSUserHostNameAndVersionInfo()
			clientInfo.ClientName = ch.config.Database + " " + ch.config.ClientName
		} else {
			clientInfo.QueryKind = QureyKindSecondaryQuery
		}
		clientInfo.Write(ch)
	}

	// todo setting
	ch.writer.String("")

	ch.writer.Uvarint(uint64(stage))

	//todo comprestion
	ch.writer.Uvarint(0)

	ch.writer.String(query)

	return ch.SendData(NewBlock(), "")

}

func (ch *Conn) SendData(block *Block, name string) error {
	ch.writer.Uvarint(clientData)
	ch.writer.String(name)
	block.write(ch)
	_, err := ch.writer.WriteTo(ch.conn)
	return err
}

// todo
func (ch *Conn) Close(ctx context.Context) error {
	if ch.status == connStatusClosed {
		return nil
	}
	ch.status = connStatusClosed
	// todo
	// if ctx != context.Background() {
	// 	ch.contextWatcher.Watch(ctx)
	// 	defer ch.contextWatcher.Unwatch()
	// }
	return ch.conn.Close()
}

// todo
func (ch *Conn) readTableColumn() {
	ch.reader.String()
	ch.reader.String()
}
func (ch *Conn) ReciveAndProccessData() (interface{}, error) {
	packet, err := ch.reader.Uvarint()
	if err != nil {
		return nil, err
	}
	switch packet {
	case serverData, serverTotals, serverExtremes:
		block := NewBlock()
		err := block.Read(ch)
		return block, err
	case serverHello:
		if err := ch.ServerInfo.Read(ch.reader); err != nil {
			return nil, err
		}
		return ch.ServerInfo, nil
	case serverPong:
		return ch.ReciveAndProccessData()
	case serverProfileInfo:
		profile := NewProfile()

		err := profile.Read(ch)
		return profile, err
	case serverProgress:
		progress := NewProgress()

		err := progress.Read(ch)
		return progress, err
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

		return ch.ReciveAndProccessData()
	}
	fmt.Println("packet not impliment", packet)
	return nil, nil
}

// Insert send query for insert and prepare insert stmt
func (ch *Conn) Insert(ctx context.Context, query string) (*InsertStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}
	err = ch.SendQueryWithOption(ctx, query, "", QueryProcessingStageComplete, nil, nil)
	if err != nil {
		return nil, err
	}
	res, err := ch.ReciveAndProccessData()

	// todo check response is block

	block := res.(*Block)
	err = block.initForInsert(ch)
	if err != nil {
		return nil, err
	}
	return &InsertStmt{
		Block:      block,
		conn:       ch,
		query:      query,
		queryID:    "",
		stage:      QueryProcessingStageComplete,
		settings:   nil,
		clientInfo: nil,
	}, nil
}

// Select send query for select and prepare SelectStmt
func (ch *Conn) Select(ctx context.Context, query string) (*SelectStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}

	err = ch.SendQueryWithOption(ctx, query, "", QueryProcessingStageComplete, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SelectStmt{
		conn:       ch,
		query:      query,
		queryID:    "",
		stage:      QueryProcessingStageComplete,
		settings:   nil,
		clientInfo: nil,
	}, nil
}
