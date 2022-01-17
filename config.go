package chconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultUsername = "default"
const defaultDatabase = "default"
const defaultDBPort = "9000"
const defaultClientName = "chx"

// AfterConnectFunc is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
// or prepare statements). If this returns an error the connection attempt fails.
type AfterConnectFunc func(ctx context.Context, conn Conn) error

// ValidateConnectFunc is called during a connection attempt after a successful authentication with the ClickHouse server.
// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
// fallback config is tried. This allows implementing high availability behavior.
type ValidateConnectFunc func(ctx context.Context, conn Conn) error

// Config is the settings used to establish a connection to a ClickHouse server. It must be created by ParseConfig and
// then it can be modified. A manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	Host           string // host (e.g. localhost)
	Port           uint16
	Database       string
	User           string
	Password       string
	ClientName     string
	TLSConfig      *tls.Config // nil disables TLS
	ConnectTimeout time.Duration
	DialFunc       DialFunc   // e.g. net.Dialer.DialContext
	LookupFunc     LookupFunc // e.g. net.Resolver.LookupHost
	ReaderFunc     ReaderFunc // e.g. bufio.Reader
	Compress       bool
	WriterFunc     WriterFunc
	// Run-time parameters to set on connection as session default values (e.g. search_path or application_name)
	RuntimeParams map[string]string

	Fallbacks []*FallbackConfig

	// ValidateConnect is called during a connection attempt after a successful authentication with the ClickHouse server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback config is tried. This allows implementing high availability behavior.
	ValidateConnect ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	AfterConnect AfterConnectFunc

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.

	// Original connection string that was parsed into config.
	connString string
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the TLSConfig field:
// according to the tls.Config docs it must not be modified after creation.
func (c *Config) Copy() *Config {
	newConf := new(Config)
	*newConf = *c
	if newConf.TLSConfig != nil {
		newConf.TLSConfig = c.TLSConfig.Clone()
	}
	if newConf.RuntimeParams != nil {
		newConf.RuntimeParams = make(map[string]string, len(c.RuntimeParams))
		for k, v := range c.RuntimeParams {
			newConf.RuntimeParams[k] = v
		}
	}
	if newConf.Fallbacks != nil {
		newConf.Fallbacks = make([]*FallbackConfig, len(c.Fallbacks))
		for i, fallback := range c.Fallbacks {
			newFallback := new(FallbackConfig)
			*newFallback = *fallback
			if newFallback.TLSConfig != nil {
				newFallback.TLSConfig = fallback.TLSConfig.Clone()
			}
			newConf.Fallbacks[i] = newFallback
		}
	}
	return newConf
}

// ConnString returns the original connection string used to connect to the ClickHouse server.
func (c *Config) ConnString() string { return c.connString }

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer and high availability (HA) connections.
type FallbackConfig struct {
	Host      string // host (e.g. localhost)
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

// ParseConfig builds a []*Config with default values and use CH* Env.
//
//   # Example DSN
//   user=vahid password=secret host=ch.example.com port=5432 dbname=mydb sslmode=verify-ca
//
//   # Example URL
//   clickhouse://vahid:secret@ch.example.com:9000/mydb?sslmode=verify-ca
//
// ParseConfig supports specifying multiple hosts in similar manner to libpq. Host and port may include comma separated
// values that will be tried in order. This can be used as part of a high availability system.
//
//   # Example URL
//   clickhouse://vahid:secret@foo.example.com:9000,bar.example.com:9000/mydb
//
// ParseConfig currently recognizes the following environment variable and their parameter key word equivalents passed
// via database URL or DSN:
//
//   CHHOST
//   CHPORT
//   CHDATABASE
//   CHUSER
//   CHPASSWORD
//   CHCLIENTNAME
//   CHCONNECT_TIMEOUT
//   CHSSLMODE
//   CHSSLKEY
//   CHSSLCERT
//   CHSSLROOTCERT
//
// Important Security Notes:
//
// ParseConfig tries to match libpq behavior with regard to CHSSLMODE. This includes defaulting to "prefer" behavior if
// not set.
//
// See http://www.postgresql.org/docs/12/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION for details on what level of
// security each sslmode provides.
//
// The sslmode "prefer" (the default), sslmode "allow", and multiple hosts are implemented via the Fallbacks field of
// the Config struct. If TLSConfig is manually changed it will not affect the fallbacks. For example, in the case of
// sslmode "prefer" this means it will first try the main Config settings which use TLS, then it will try the fallback
// which does not use TLS. This can lead to an unexpected unencrypted connection if the main TLS config is manually
// changed later but the unencrypted fallback is present. Ensure there are no stale fallbacks when manually setting
// TLCConfig.
//
//
// If a host name resolves into multiple addresses chconn will only try the first.
//
func ParseConfig(connString string) (*Config, error) {
	defaultSettings := defaultSettings()
	envSettings := parseEnvSettings()

	connStringSettings := make(map[string]string)
	if connString != "" {
		var err error
		// connString may be a database URL or a DSN
		if strings.HasPrefix(connString, "clickhouse://") {
			connStringSettings, err = parseURLSettings(connString)
			if err != nil {
				return nil, &parseConfigError{connString: connString, msg: "failed to parse as URL", err: err}
			}
		} else {
			connStringSettings, err = parseDSNSettings(connString)
			if err != nil {
				return nil, &parseConfigError{connString: connString, msg: "failed to parse as DSN", err: err}
			}
		}
	}

	settings := mergeSettings(defaultSettings, envSettings, connStringSettings)

	config := &Config{
		createdByParseConfig: true,
		Database:             settings["database"],
		User:                 settings["user"],
		Password:             settings["password"],
		RuntimeParams:        make(map[string]string),
		ClientName:           settings["client_name"],
		connString:           connString,
	}

	if compress, err := strconv.ParseBool(settings["compress"]); err == nil && compress {
		config.Compress = true
	}

	if connectTimeoutSetting, present := settings["connect_timeout"]; present {
		connectTimeout, err := parseConnectTimeoutSetting(connectTimeoutSetting)
		if err != nil {
			return nil, &parseConfigError{connString: connString, msg: "invalid connect_timeout", err: err}
		}
		config.ConnectTimeout = connectTimeout
		config.DialFunc = makeConnectTimeoutDialFunc(connectTimeout)
	} else {
		defaultDialer := makeDefaultDialer()
		config.DialFunc = defaultDialer.DialContext
	}

	config.LookupFunc = makeDefaultResolver().LookupHost

	notRuntimeParams := map[string]struct{}{
		"host":            {},
		"port":            {},
		"database":        {},
		"user":            {},
		"password":        {},
		"connect_timeout": {},
		"sslmode":         {},
		"client_name":     {},
		"sslkey":          {},
		"sslcert":         {},
		"sslrootcert":     {},
		"compress":        {},
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

		tlsConfigs, err = configTLS(settings, host)

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

func mergeSettings(settingSets ...map[string]string) map[string]string {
	settings := make(map[string]string)

	for _, s2 := range settingSets {
		for k, v := range s2 {
			settings[k] = v
		}
	}

	return settings
}

func parseEnvSettings() map[string]string {
	settings := make(map[string]string)

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

	return settings
}

func parseURLSettings(connString string) (map[string]string, error) {
	settings := make(map[string]string)

	urlConn, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}

	if urlConn.User != nil {
		settings["user"] = urlConn.User.Username()
		if password, present := urlConn.User.Password(); present {
			settings["password"] = password
		}
	}

	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(urlConn.Host, ",") {
		if host == "" {
			continue
		}
		if isIPOnly(host) {
			hosts = append(hosts, strings.Trim(host, "[]"))
			continue
		}
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return nil, fmt.Errorf("failed to split host:port in '%s', err: %w", host, err)
		}
		if h != "" {
			hosts = append(hosts, h)
		}
		if p != "" {
			ports = append(ports, p)
		}
	}
	if len(hosts) > 0 {
		settings["host"] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		settings["port"] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(urlConn.Path, "/")
	if database != "" {
		settings["database"] = database
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	for k, v := range urlConn.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		settings[k] = v[0]
	}

	return settings, nil
}

func isIPOnly(host string) bool {
	return net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":")
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func parseDSNSettings(s string) (map[string]string, error) {
	settings := make(map[string]string)

	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return nil, ErrInvalidDSN
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		if s == "" {
		} else if s[0] != '\'' {
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, ErrInvalidBackSlash
					}
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
				return nil, ErrInvalidquoted
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

		if key == "" {
			return nil, ErrInvalidDSN
		}

		settings[key] = val
	}

	return settings, nil
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
//nolint:funlen,gocyclo
func configTLS(settings map[string]string, thisHost string) ([]*tls.Config, error) {
	host := thisHost
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]

	// in clickhouse default non tls connection accepted and  tls connection listen on another port
	if sslmode == "" || sslmode == "disable" {
		return []*tls.Config{nil}, nil
	}

	//nolint:gosec // it change by config
	tlsConfig := &tls.Config{}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		if sslrootcert != "" {
			goto nextCase
		}
		tlsConfig.InsecureSkipVerify = true
		break
	nextCase:
		fallthrough
	case "verify-ca":
		// Don't perform the default certificate verification because it
		// will verify the hostname. Instead, verify the server's
		// certificate chain ourselves in VerifyPeerCertificate and
		// ignore the server name. This emulates libpq's verify-ca
		// behavior.
		//
		// See https://github.com/golang/go/issues/21971#issuecomment-332693931
		// and https://pkg.go.dev/crypto/tls?tab=doc#example-Config-VerifyPeerCertificate
		// for more info.
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(certificates [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(certificates))
			for i, asn1Data := range certificates {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return fmt.Errorf("failed to parse certificate from server: %w", err)
				}
				certs[i] = cert
			}

			// Leave DNSName empty to skip hostname verification.
			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				Intermediates: x509.NewCertPool(),
			}
			// Skip the first cert because it's the leaf. All others
			// are intermediates.
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, ErrSSLModeInvalid
	}

	if sslrootcert != "" {
		caCertPool := x509.NewCertPool()

		caPath := sslrootcert
		caCert, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA file: %w", err)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, ErrAddCA
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, ErrMissCertRequirement
	}

	if sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return nil, fmt.Errorf("unable to read cert: %w", err)
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
		return 0, ErrPortInvalid
	}
	return uint16(port), nil
}

func makeDefaultDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func makeDefaultResolver() *net.Resolver {
	return net.DefaultResolver
}

func parseConnectTimeoutSetting(s string) (time.Duration, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if timeout < 0 {
		return 0, ErrNegativeTimeout
	}
	return time.Duration(timeout) * time.Second, nil
}

func makeConnectTimeoutDialFunc(timeout time.Duration) DialFunc {
	d := makeDefaultDialer()
	d.Timeout = timeout
	return d.DialContext
}
