package chconn

import (
	"crypto/tls"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var parseConfigTests = []struct {
	name       string
	connString string
	config     *Config
}{
	// Test all sslmodes
	{
		name:       "sslmode not set (prefer)",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "localhost",
			Port:       9000,
			Database:   "mydb",
			ClientName: defaultClientName,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "localhost",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "sslmode disable",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			ClientName:    defaultClientName,
			Port:          9000,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "sslmode allow",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=allow",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host: "localhost",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			},
		},
	},
	{
		name:       "sslmode prefer",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=prefer",
		config: &Config{

			User:       "vahid",
			Password:   "secret",
			Host:       "localhost",
			Port:       9000,
			Database:   "mydb",
			ClientName: defaultClientName,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "localhost",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "sslmode require",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=require",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			Database:      "mydb",
			ClientName:    defaultClientName,
			RuntimeParams: map[string]string{},
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	},
	{
		name:       "sslmode verify-ca",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=verify-ca",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     &tls.Config{ServerName: "localhost"},
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "sslmode verify-full",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=verify-full",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     &tls.Config{ServerName: "localhost"},
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "database url everything",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=disable&client_name=chxtest&extradata=test",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "localhost",
			Port:       9000,
			Database:   "mydb",
			TLSConfig:  nil,
			ClientName: "chxtest",
			RuntimeParams: map[string]string{
				"extradata": "test",
			},
		},
	},
	{
		name:       "database url missing password",
		connString: "clickhouse://vahid@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "database url missing user and password",
		connString: "clickhouse://localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:          defaultUsername,
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "database url missing port",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "database url clickhouse protocol",
		connString: "clickhouse://vahid@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN everything",
		connString: "user=vahid password=secret host=localhost port=9000 dbname=mydb sslmode=disable client_name=chxtest",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			Database:      "mydb",
			TLSConfig:     nil,
			ClientName:    "chxtest",
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with escaped single quote",
		connString: "user=vahid\\'s password=secret host=localhost port=9000 dbname=mydb sslmode=disable",
		config: &Config{
			User:          "vahid's",
			Password:      "secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with escaped backslash",
		connString: "user=vahid password=sooper\\\\secret host=localhost port=9000 dbname=mydb sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "sooper\\secret",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with single quoted values",
		connString: "user='vahid' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:          "vahid",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with single quoted value with escaped single quote",
		connString: "user='vahid\\'s' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:          "vahid's",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with empty single quoted value",
		connString: "user='vahid' password='' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:          "vahid",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "DSN with space between key and value",
		connString: "user = 'vahid' password = '' host = 'localhost' dbname = 'mydb' sslmode='disable'",
		config: &Config{
			User:          "vahid",
			Host:          "localhost",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	},
	{
		name:       "URL multiple hosts",
		connString: "clickhouse://vahid:secret@foo,bar,baz/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "foo",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host:      "baz",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "URL multiple hosts and ports",
		connString: "clickhouse://vahid:secret@foo:1,bar:2,baz:3/mydb?sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "foo",
			Port:          1,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "bar",
					Port:      2,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host:      "baz",
					Port:      3,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "DSN multiple hosts one port",
		connString: "user=vahid password=secret host=foo,bar,baz port=9000 dbname=mydb sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "foo",
			Port:          9000,
			ClientName:    defaultClientName,
			Database:      "mydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host:      "baz",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "DSN multiple hosts multiple ports",
		connString: "user=vahid password=secret host=foo,bar,baz port=1,2,3 dbname=mydb sslmode=disable",
		config: &Config{
			User:          "vahid",
			Password:      "secret",
			Host:          "foo",
			Port:          1,
			Database:      "mydb",
			TLSConfig:     nil,
			ClientName:    defaultClientName,
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "bar",
					Port:      2,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host:      "baz",
					Port:      3,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "multiple hosts and fallback tsl",
		connString: "user=vahid password=secret host=foo,bar,baz dbname=mydb sslmode=prefer",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "foo",
			Port:       9000,
			Database:   "mydb",
			ClientName: defaultClientName,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "foo",
					Port:      9000,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host: "bar",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				&FallbackConfig{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				&FallbackConfig{
					Host: "baz",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				&FallbackConfig{
					Host:      "baz",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
}

func TestParseConfig(t *testing.T) {
	t.Parallel()

	for i, tt := range parseConfigTests {
		config, err := ParseConfig(tt.connString)
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func assertConfigsEqual(t *testing.T, expected, actual *Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ClientName, actual.ClientName, "%s - Client Name", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}

var parseConfigEnvLibpqTests = []struct {
	name    string
	envvars map[string]string
	config  *Config
}{
	{
		// not testing no environment at all as that would use default host and that can vary.
		name:    "CHHOST only",
		envvars: map[string]string{"CHHOST": "123.123.123.123"},
		config: &Config{
			User:       defaultUsername,
			Host:       "123.123.123.123",
			Port:       9000,
			ClientName: defaultClientName,
			Database:   defaultDatabase,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams: map[string]string{},
			Fallbacks: []*FallbackConfig{
				&FallbackConfig{
					Host:      "123.123.123.123",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name: "All non-TLS environment",
		envvars: map[string]string{
			"CHHOST":            "123.123.123.123",
			"CHPORT":            "7777",
			"CHDATABASE":        "foo",
			"CHUSER":            "bar",
			"CHPASSWORD":        "baz",
			"CHCONNECT_TIMEOUT": "10",
			"CHSSLMODE":         "disable",
			"CHCLIENTNAME":      "chxtest",
		},
		config: &Config{
			Host:          "123.123.123.123",
			Port:          7777,
			Database:      "foo",
			User:          "bar",
			Password:      "baz",
			TLSConfig:     nil,
			ClientName:    "chxtest",
			RuntimeParams: map[string]string{},
		},
	},
}

func TestParseConfigEnvLibpq(t *testing.T) {

	chEnvvars := []string{"CHHOST", "CHPORT", "CHDATABASE", "CHUSER", "CHPASSWORD", "CHCLIENTNAME", "CHSSLMODE", "CHCONNECT_TIMEOUT"}

	savedEnv := make(map[string]string)
	for _, n := range chEnvvars {
		savedEnv[n] = os.Getenv(n)
	}
	defer func() {
		for k, v := range savedEnv {
			err := os.Setenv(k, v)
			if err != nil {
				t.Fatalf("Unable to restore environment: %v", err)
			}
		}
	}()

	for i, tt := range parseConfigEnvLibpqTests {
		for _, n := range chEnvvars {
			err := os.Unsetenv(n)
			require.NoError(t, err)
		}

		for k, v := range tt.envvars {
			err := os.Setenv(k, v)
			require.NoError(t, err)
		}

		config, err := ParseConfig("")
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}
