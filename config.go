package chconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"
)

const defaultUsername = "default"
const defaultDatabase = "default"
const defaultDBPort = "9000"
const defaultClientName = "chx"

type AfterConnectFunc func(ctx context.Context, conn *Conn) error
type ValidateConnectFunc func(ctx context.Context, conn *Conn) error

// Config is the settings used to establish a connection to a ClickHouse server. It must be created by ParseConfig and
// then it can be modified. A manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	Host          string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port          uint16
	Database      string
	User          string
	Password      string
	ClientName    string            // e.g. net.Resolver.LookupHost
	TLSConfig     *tls.Config       // nil disables TLS
	DialFunc      DialFunc          // e.g. net.Dialer.DialContext
	LookupFunc    LookupFunc        // e.g. net.Resolver.LookupHost
	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)

	Fallbacks []*FallbackConfig

	// ValidateConnect is called during a connection attempt after a successful authentication with the ClickHouse server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback config is tried. This allows implementing high availability behavior.
	ValidateConnect ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	AfterConnect AfterConnectFunc

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer and high availability (HA) connections.
type FallbackConfig struct {
	Host      string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port      uint16
	TLSConfig *tls.Config // nil disables TLS
}

// NetworkAddress converts a ClickHouse host and port into network and address suitable for use with
// net.Dial.
func NetworkAddress(host string, port uint16) (network, address string) {
	network = "tcp"
	address = net.JoinHostPort(host, strconv.Itoa(int(port)))
	return
}

// todo comment
func ParseConfig(connString string) (*Config, error) {
	settings := defaultSettings()
	addEnvSettings(settings)

	if connString != "" {
		// connString may be a database URL or a DSN
		if strings.HasPrefix(connString, "clickhouse://") {
			err := addURLSettings(settings, connString)
			if err != nil {
				return nil, &parseConfigError{connString: connString, msg: "failed to parse as URL", err: err}
			}
		} else {
			err := addDSNSettings(settings, connString)
			if err != nil {
				return nil, &parseConfigError{connString: connString, msg: "failed to parse as DSN", err: err}
			}
		}
	}

	config := &Config{
		createdByParseConfig: true,
		Database:             settings["database"],
		User:                 settings["user"],
		Password:             settings["password"],
		RuntimeParams:        make(map[string]string),
		ClientName:           settings["client_name"],
	}

	if connectTimeout, present := settings["connect_timeout"]; present {
		dialFunc, err := makeConnectTimeoutDialFunc(connectTimeout)
		if err != nil {
			return nil, &parseConfigError{connString: connString, msg: "invalid connect_timeout", err: err}
		}
		config.DialFunc = dialFunc
	} else {
		defaultDialer := makeDefaultDialer()
		config.DialFunc = defaultDialer.DialContext
	}

	config.LookupFunc = makeDefaultResolver().LookupHost

	notRuntimeParams := map[string]struct{}{
		"host":                 struct{}{},
		"port":                 struct{}{},
		"database":             struct{}{},
		"user":                 struct{}{},
		"password":             struct{}{},
		"passfile":             struct{}{},
		"connect_timeout":      struct{}{},
		"sslmode":              struct{}{},
		"client_name":          struct{}{},
		"sslkey":               struct{}{},
		"sslcert":              struct{}{},
		"sslrootcert":          struct{}{},
		"target_session_attrs": struct{}{},
		"min_read_buffer_size": struct{}{},
	}

	for k, v := range settings {
		if _, present := notRuntimeParams[k]; present {
			continue
		}
		config.RuntimeParams[k] = v
	}

	fallbacks := []*FallbackConfig{}

	hosts := strings.Split(settings["host"], ",")
	ports := strings.Split(settings["port"], ",")

	for i, host := range hosts {
		var portStr string
		if i < len(ports) {
			portStr = ports[i]
		} else {
			portStr = ports[0]
		}

		port, err := parsePort(portStr)
		if err != nil {
			return nil, &parseConfigError{connString: connString, msg: "invalid port", err: err}
		}

		var tlsConfigs []*tls.Config

		tlsConfigs, err = configTLS(settings)
		if err != nil {
			return nil, &parseConfigError{connString: connString, msg: "failed to configure TLS", err: err}
		}

		for _, tlsConfig := range tlsConfigs {
			fallbacks = append(fallbacks, &FallbackConfig{
				Host:      host,
				Port:      port,
				TLSConfig: tlsConfig,
			})
		}
	}

	config.Host = fallbacks[0].Host
	config.Port = fallbacks[0].Port
	config.TLSConfig = fallbacks[0].TLSConfig
	config.Fallbacks = fallbacks[1:]

	return config, nil
}

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = "localhost"
	settings["port"] = defaultDBPort
	settings["user"] = defaultUsername
	settings["database"] = defaultDatabase
	settings["client_name"] = defaultClientName

	return settings
}

func addEnvSettings(settings map[string]string) {
	nameMap := map[string]string{
		"CHHOST":            "host",
		"CHPORT":            "port",
		"CHDATABASE":        "database",
		"CHUSER":            "user",
		"CHPASSWORD":        "password",
		"CHCLIENTNAME":      "client_name",
		"CHCONNECT_TIMEOUT": "connect_timeout",
		"CHSSLMODE":         "sslmode",
		"CHSSLKEY":          "sslkey",
		"CHSSLCERT":         "sslcert",
		"CHSSLROOTCERT":     "sslrootcert",
	}

	for envname, realname := range nameMap {
		value := os.Getenv(envname)
		if value != "" {
			settings[realname] = value
		}
	}
}

func addURLSettings(settings map[string]string, connString string) error {
	url, err := url.Parse(connString)
	if err != nil {
		return err
	}

	if url.User != nil {
		settings["user"] = url.User.Username()
		if password, present := url.User.Password(); present {
			settings["password"] = password
		}
	}

	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(url.Host, ",") {
		parts := strings.SplitN(host, ":", 2)
		if parts[0] != "" {
			hosts = append(hosts, parts[0])
		}
		if len(parts) == 2 {
			ports = append(ports, parts[1])
		}
	}
	if len(hosts) > 0 {
		settings["host"] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		settings["port"] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(url.Path, "/")
	if database != "" {
		settings["database"] = database
	}

	for k, v := range url.Query() {
		settings[k] = v[0]
	}

	return nil
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func addDSNSettings(settings map[string]string, s string) error {
	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return errors.New("invalid dsn")
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		if s[0] != '\'' {
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		} else { // quoted string
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return errors.New("unterminated quoted string in connection info string")
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		}

		if k, ok := nameMap[key]; ok {
			key = k
		}

		settings[key] = val
	}

	return nil
}

type chTLSArgs struct {
	sslMode     string
	sslRootCert string
	sslCert     string
	sslKey      string
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
func configTLS(settings map[string]string) ([]*tls.Config, error) {
	host := settings["host"]
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]

	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}

	tlsConfig := &tls.Config{}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		tlsConfig.InsecureSkipVerify = sslrootcert == ""
	case "verify-ca", "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, errors.New("sslmode is invalid")
	}

	if sslrootcert != "" {
		caCertPool := x509.NewCertPool()

		caPath := sslrootcert
		caCert, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, errors.Errorf("unable to read CA file: %w", err)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.Errorf("unable to add CA to cert pool: %w", err)
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	if sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return nil, errors.Errorf("unable to read cert: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	switch sslmode {
	case "allow":
		return []*tls.Config{nil, tlsConfig}, nil
	case "prefer":
		return []*tls.Config{tlsConfig, nil}, nil
	case "require", "verify-ca", "verify-full":
		return []*tls.Config{tlsConfig}, nil
	default:
		panic("BUG: bad sslmode should already have been caught")
	}
}

func parsePort(s string) (uint16, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > math.MaxUint16 {
		return 0, errors.New("outside range")
	}
	return uint16(port), nil
}

func makeDefaultDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func makeDefaultResolver() *net.Resolver {
	return net.DefaultResolver
}

func makeConnectTimeoutDialFunc(s string) (DialFunc, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("negative timeout")
	}

	d := makeDefaultDialer()
	d.Timeout = time.Duration(timeout) * time.Second
	return d.DialContext, nil
}
