module github.com/vahid-sohrabloo/chconn/v3

go 1.26.0

// Go 1.26 tightened net/url.Parse to reject colons in the host, which rejects
// chconn's multi-host connection strings ("host1:9000,host2:9000") with
// `invalid port ... after host`. Go 1.27 narrowed the check and accepts them
// again, so this only affects Go 1.26. Declaring go >= 1.26 opts into the
// strict behavior, so pin it off until the floor is Go 1.27.
godebug urlstrictcolons=0

tool github.com/vahid-sohrabloo/chconn/v3/cmd/chgen

require (
	github.com/go-faster/city v1.0.1
	github.com/jackc/puddle/v2 v2.2.2
	github.com/kelindar/bitmap v1.5.5
	github.com/klauspost/compress v1.19.2
	github.com/pierrec/lz4/v4 v4.1.29
	github.com/stretchr/testify v1.12.1
	golang.org/x/tools v0.49.0
)

require (
	github.com/kelindar/simd v1.2.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.39.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
