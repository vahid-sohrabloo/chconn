package column_test

import (
	"io"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// chVersionAtLeast returns true if the server version is >= major.minor.
func chVersionAtLeast(info *shared.ServerInfo, major, minor uint64) bool {
	return info.MajorVersion > major || (info.MajorVersion == major && info.MinorVersion >= minor)
}

// jsonSettings returns the appropriate settings for JSON tests based on CH version.
// JSON and Dynamic became GA in CH 25.3, so experimental flags are only needed before that.
func jsonSettings(info *shared.ServerInfo) chconn.Settings {
	if chVersionAtLeast(info, 25, 3) {
		return nil
	}
	return chconn.Settings{
		{Name: "allow_experimental_json_type", Value: "true"},
		{Name: "allow_experimental_dynamic_type", Value: "true"},
	}
}

// jsonStringSettings returns settings for JSON string serialization mode.
func jsonStringSettings(info *shared.ServerInfo) chconn.Settings {
	s := jsonSettings(info)
	return append(s, chconn.Setting{
		Name: "output_format_native_write_json_as_string", Value: "1",
	})
}

// dynamicSettings returns the appropriate settings for Dynamic tests.
func dynamicSettings(info *shared.ServerInfo) chconn.Settings {
	if chVersionAtLeast(info, 25, 3) {
		return nil
	}
	return chconn.Settings{
		{Name: "allow_experimental_dynamic_type", Value: "true"},
	}
}

// variantSettings returns the appropriate settings for Variant tests.
func variantSettings(info *shared.ServerInfo) chconn.Settings {
	if chVersionAtLeast(info, 25, 3) {
		return chconn.Settings{
			{Name: "allow_suspicious_low_cardinality_types", Value: "true"},
		}
	}
	return chconn.Settings{
		{Name: "allow_suspicious_low_cardinality_types", Value: "true"},
		{Name: "allow_experimental_variant_type", Value: "1"},
	}
}

// skipIfCHBelow skips the test if the server version is below major.minor.
func skipIfCHBelow(t *testing.T, info *shared.ServerInfo, major, minor uint64, feature string) {
	t.Helper()
	if !chVersionAtLeast(info, major, minor) {
		t.Skipf("ClickHouse %d.%d does not support %s (need %d.%d+)",
			info.MajorVersion, info.MinorVersion, feature, major, minor)
	}
}

type readErrorHelper struct {
	numberValid int
	err         error
	r           io.Reader
	count       int
}

func (r *readErrorHelper) Read(p []byte) (int, error) {
	r.count++
	if r.count > r.numberValid {
		return 0, r.err
	}
	return r.r.Read(p)
}

type writerErrorHelper struct {
	numberValid int
	err         error
	w           io.Writer
	count       int
}

func (w *writerErrorHelper) Write(p []byte) (int, error) {
	w.count++
	if w.count > w.numberValid {
		return 0, w.err
	}
	return w.w.Write(p)
}
