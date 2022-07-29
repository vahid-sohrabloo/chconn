package chconn

// Setting is a setting for the clickhouse query.
//
// The list of setting is here: https://clickhouse.com/docs/en/operations/settings/settings/
// Some of settings doesn't have effect. for example `http_zlib_compression_level`
// because chconn use TCP connection to send data not HTTP.
type Setting struct {
	Important   bool
	Name, Value string
}

// Settings is a list of settings for the clickhouse query.
type Settings []Setting
