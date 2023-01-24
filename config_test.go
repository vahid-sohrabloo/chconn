package chconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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
		name:       "sslmode not set (disable)",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			Database:       "mydb",
			ClientName:     defaultClientName,
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "sslmode disable",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			ClientName:     defaultClientName,
			Port:           9000,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "sslmode allow",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=allow",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
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
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
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
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			Database:       "mydb",
			ClientName:     defaultClientName,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	},
	{
		name:       "sslmode verify-ca",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=verify-ca",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "localhost",
			Port:       9000,
			ClientName: defaultClientName,
			Database:   "mydb",
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "sslmode verify-full",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=verify-full",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      &tls.Config{ServerName: "localhost"},
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url everything",
		connString: "clickhouse://vahid:secret@localhost:9000/mydb?sslmode=disable&client_name=chxtest&extradata=test&connect_timeout=5",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			Database:       "mydb",
			TLSConfig:      nil,
			ConnectTimeout: 5 * time.Second,
			ClientName:     "chxtest",
			RuntimeParams: map[string]string{
				"extradata": "test",
			},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url missing password",
		connString: "clickhouse://vahid@localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url missing user and password",
		connString: "clickhouse://localhost:9000/mydb?sslmode=disable",
		config: &Config{
			User:           defaultUsername,
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
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
			User:           "vahid",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url IPv4 with port",
		connString: "clickhouse://vahid@127.0.0.1:5433/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Host:           "127.0.0.1",
			ClientName:     defaultClientName,
			Port:           5433,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url IPv6 with port",
		connString: "clickhouse://vahid@[2001:db8::1]:5433/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Host:           "2001:db8::1",
			Port:           5433,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "database url IPv6 no port",
		connString: "clickhouse://vahid@[2001:db8::1]/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Host:           "2001:db8::1",
			Port:           9000,
			Database:       "mydb",
			ClientName:     defaultClientName,
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "DSN everything",
		connString: "user=vahid password=secret host=localhost port=9000 dbname=mydb sslmode=disable client_name=chxtest connect_timeout=5",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "localhost",
			Port:           9000,
			Database:       "mydb",
			TLSConfig:      nil,
			ClientName:     "chxtest",
			ConnectTimeout: 5 * time.Second,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
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
			User:           "vahid",
			Password:       "sooper\\secret",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "DSN with single quoted values",
		connString: "user='vahid' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:           "vahid",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "DSN with single quoted value with escaped single quote",
		connString: "user='vahid\\'s' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:           "vahid's",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "DSN with empty single quoted value",
		connString: "user='vahid' password='' host='localhost' dbname='mydb' sslmode='disable'",
		config: &Config{
			User:           "vahid",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "DSN with space between key and value",
		connString: "user = 'vahid' password = '' host = 'localhost' dbname = 'mydb' sslmode='disable'",
		config: &Config{
			User:           "vahid",
			Host:           "localhost",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
		},
	},
	{
		name:       "URL multiple hosts",
		connString: "clickhouse://vahid:secret@foo,bar,baz/mydb?sslmode=disable",
		config: &Config{
			User:           "vahid",
			Password:       "secret",
			Host:           "foo",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				{
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
			User:           "vahid",
			Password:       "secret",
			Host:           "foo",
			Port:           1,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "bar",
					Port:      2,
					TLSConfig: nil,
				},
				{
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
			User:           "vahid",
			Password:       "secret",
			Host:           "foo",
			Port:           9000,
			ClientName:     defaultClientName,
			Database:       "mydb",
			TLSConfig:      nil,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				{
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
			User:           "vahid",
			Password:       "secret",
			Host:           "foo",
			Port:           1,
			Database:       "mydb",
			TLSConfig:      nil,
			ClientName:     defaultClientName,
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "bar",
					Port:      2,
					TLSConfig: nil,
				},
				{
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
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "foo",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "bar",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "baz",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
					Host:      "baz",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	},
	{
		name:       "enable compress",
		connString: "user=vahid password=secret host=foo,bar,baz dbname=mydb sslmode=prefer compress=checksum use_write_buffer=false",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "foo",
			Port:       9000,
			Database:   "mydb",
			Compress:   CompressChecksum,
			ClientName: defaultClientName,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: true,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "foo",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "bar",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "baz",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
					Host:      "baz",
					Port:      9000,
					TLSConfig: nil,
				},
			},
		},
	}, {
		name:       "disable write buffer",
		connString: "user=vahid password=secret host=foo,bar,baz dbname=mydb sslmode=prefer use_write_buffer=false",
		config: &Config{
			User:       "vahid",
			Password:   "secret",
			Host:       "foo",
			Port:       9000,
			Database:   "mydb",
			Compress:   CompressNone,
			ClientName: defaultClientName,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			RuntimeParams:  map[string]string{},
			UseWriteBuffer: false,
			Fallbacks: []*FallbackConfig{
				{
					Host:      "foo",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "bar",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
					Host:      "bar",
					Port:      9000,
					TLSConfig: nil,
				},
				{
					Host: "baz",
					Port: 9000,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					}},
				{
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

func TestParseConfigDSNWithTrailingEmptyEqualDoesNotPanic(t *testing.T) {
	_, err := ParseConfig("host= user= password= port= database=")
	require.NoError(t, err)
}

func TestParseConfigDSNLeadingEqual(t *testing.T) {
	_, err := ParseConfig("= user=vahid")
	require.Error(t, err)
}

func TestParseConfigDSNTrailingBackslash(t *testing.T) {
	_, err := ParseConfig(`x=x\`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid backslash")
}

func TestConfigCopyReturnsEqualConfig(t *testing.T) {
	connString := "clickhouse://vahid:secret@localhost:9000/mydb?client_name=chxtest&connect_timeout=5"
	original, err := ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")
}

func TestConfigCopyOriginalConfigDidNotChange(t *testing.T) {
	connString := "host=localhost,localhost2 port=9000,9000 database=mydb  client_name=chxtest connect_timeout=5"
	original, err := ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")

	copied.Port = uint16(5433)
	copied.RuntimeParams["foo"] = "bar"

	assert.Equal(t, uint16(9000), original.Port)
	assert.Equal(t, "", original.RuntimeParams["foo"])
}

func TestConfigCopyCanBeUsedToConnect(t *testing.T) {
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	original, err := ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assert.NotPanics(t, func() {
		_, err = ConnectConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
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
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.ClientName, actual.ClientName, "%s - Client Name", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t,
				expected.TLSConfig.InsecureSkipVerify,
				actual.TLSConfig.InsecureSkipVerify,
				"%s - TLSConfig InsecureSkipVerify",
				testName,
			)
			assert.Equalf(t,
				expected.TLSConfig.ServerName,
				actual.TLSConfig.ServerName,
				"%s - TLSConfig ServerName",
				testName,
			)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t,
				expected.Fallbacks[i].TLSConfig == nil,
				actual.Fallbacks[i].TLSConfig == nil,
				"%s - Fallback %d - TLSConfig",
				testName,
				i,
			) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t,
						expected.Fallbacks[i].TLSConfig.InsecureSkipVerify,
						actual.Fallbacks[i].TLSConfig.InsecureSkipVerify,
						"%s - Fallback %d - TLSConfig InsecureSkipVerify", testName,
					)
					assert.Equalf(t,
						expected.Fallbacks[i].TLSConfig.ServerName,
						actual.Fallbacks[i].TLSConfig.ServerName,
						"%s - Fallback %d - TLSConfig ServerName",
						testName,
					)
				}
			}
		}
	}
}

func TestParseConfigEnv(t *testing.T) {
	tests := []struct {
		name    string
		envvars map[string]string
		config  *Config
	}{
		{
			// not testing no environment at all as that would use default host and that can vary.
			name:    "CHHOST only",
			envvars: map[string]string{"CHHOST": "123.123.123.123"},
			config: &Config{
				User:           defaultUsername,
				Host:           "123.123.123.123",
				Port:           9000,
				ClientName:     defaultClientName,
				Database:       defaultDatabase,
				TLSConfig:      nil,
				RuntimeParams:  map[string]string{},
				UseWriteBuffer: true,
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
				Host:           "123.123.123.123",
				Port:           7777,
				Database:       "foo",
				User:           "bar",
				Password:       "baz",
				ConnectTimeout: 10 * time.Second,
				TLSConfig:      nil,
				ClientName:     "chxtest",
				RuntimeParams:  map[string]string{},
				UseWriteBuffer: true,
			},
		},
	}
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

	for i, tt := range tests {
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

func TestParseConfigError(t *testing.T) {
	t.Parallel()

	content := []byte("invalid tls")
	tmpInvalidTLS, err := os.CreateTemp("", "invalidtls")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpInvalidTLS.Name()) // clean up

	if _, err := tmpInvalidTLS.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpInvalidTLS.Close(); err != nil {
		t.Fatal(err)
	}

	parseConfigErrorTests := []struct {
		name       string
		connString string
		err        string
		errUnwarp  string
	}{
		{
			name:       "invalid url",
			connString: "clickhouse://invalid\t",
			err:        "cannot parse `clickhouse://invalid\t`: failed to parse as URL (parse \"clickhouse://invalid\\t\": net/url: invalid control character in URL)", //nolint:lll //can't change line lengh
		}, {
			name:       "invalid port",
			connString: "port=invalid",
			errUnwarp:  "strconv.ParseUint: parsing \"invalid\": invalid syntax",
		}, {
			name:       "invalid port range",
			connString: "port=0",
			err:        "cannot parse `port=0`: invalid port (outside range)",
		}, {
			name:       "invalid connect_timeout",
			connString: "connect_timeout=200g",
			err:        "cannot parse `connect_timeout=200g`: invalid connect_timeout (strconv.ParseInt: parsing \"200g\": invalid syntax)",
		}, {
			name:       "negative connect_timeout",
			connString: "connect_timeout=-100",
			err:        "cannot parse `connect_timeout=-100`: invalid connect_timeout (negative timeout)",
		}, {
			name:       "negative sslmode",
			connString: "sslmode=invalid",
			err:        "cannot parse `sslmode=invalid`: failed to configure TLS (sslmode is invalid)",
		}, {
			name:       "fail load sslrootcert",
			connString: "sslrootcert=invalid_address sslmode=prefer",
			err:        "cannot parse `sslrootcert=invalid_address sslmode=prefer`: failed to configure TLS (unable to read CA file: open invalid_address: no such file or directory)", //nolint:lll //can't change line lengh
		}, {
			name:       "invalid sslrootcert",
			connString: "sslrootcert=" + tmpInvalidTLS.Name() + " sslmode=prefer",
			err:        "cannot parse `sslrootcert=" + tmpInvalidTLS.Name() + " sslmode=prefer`: failed to configure TLS (unable to add CA to cert pool)", //nolint:lll //can't change line lengh
		}, {
			name:       "not provide both sslcert and sskkey",
			connString: "sslcert=invalid_address sslmode=prefer",
			err:        "cannot parse `sslcert=invalid_address sslmode=prefer`: failed to configure TLS (both \"sslcert\" and \"sslkey\" are required)", //nolint:lll //can't change line lengh
		}, {
			name:       "invalid sslcert",
			connString: "sslcert=invalid_address sslkey=invalid_address sslmode=prefer",
			err:        "cannot parse `sslcert=invalid_address sslkey=invalid_address sslmode=prefer`: failed to configure TLS (unable to read cert: open invalid_address: no such file or directory)", //nolint:lll //can't change line lengh
		},
	}

	for i, tt := range parseConfigErrorTests {
		_, err := ParseConfig(tt.connString)
		if !assert.Errorf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}
		if tt.err != "" {
			if !assert.EqualError(t, err, tt.err, "Test %d (%s)", i, tt.name) {
				continue
			}
		} else {
			if !assert.EqualErrorf(t, errors.Unwrap(err), tt.errUnwarp, "Test %d (%s)", i, tt.name) {
				continue
			}
		}
	}
}
