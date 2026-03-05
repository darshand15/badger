module github.com/dgraph-io/badger/v4

go 1.24

toolchain go1.24.1

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgraph-io/ristretto/v2 v2.1.0
	github.com/dustin/go-humanize v1.0.1
	github.com/google/flatbuffers v25.2.10+incompatible
	github.com/klauspost/compress v1.18.0
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.11.0
	go.opentelemetry.io/contrib/zpages v0.60.0
	go.opentelemetry.io/otel v1.35.0
	golang.org/x/net v0.43.0
	golang.org/x/sys v0.35.0
	google.golang.org/protobuf v1.36.8
)

require (
	github.com/apache/arrow-go/v18 v18.4.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/marcboeker/go-duckdb v1.8.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v4.0.0 // see #1888 and #1889

retract v4.3.0 // see #2113 and #2121
