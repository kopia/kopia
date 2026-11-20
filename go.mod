module github.com/kopia/kopia

go 1.27

toolchain go1.27.4

require (
	cloud.google.com/go/storage v1.70.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.24.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.15.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.3
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/chromedp/cdproto v0.160.3
	github.com/chromedp/chromedp v0.23.1
	github.com/coreos/go-systemd/v22 v22.7.0
	github.com/dustinkirkland/golang-petname v0.0.0-20260215035315-f0c533e9ce9b
	github.com/edsrzf/mmap-go v1.2.0
	github.com/fatih/color v1.19.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/gofrs/flock v0.13.2
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/fswalker v0.3.4
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hanwen/go-fuse/v2 v2.12.1
	github.com/hashicorp/cronexpr v1.1.3
	github.com/klauspost/compress v1.20.4
	github.com/klauspost/pgzip v1.3.0
	github.com/klauspost/reedsolomon v1.14.2
	github.com/kopia/htmluibuild v0.0.1-0.20261118001847-6a6aabd30284
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.16
	github.com/minio/minio-go/v7 v7.4.1
	github.com/mocktools/go-smtp-mock/v2 v2.5.4
	github.com/mxk/go-vss v1.2.1
	github.com/natefinch/atomic v1.0.1
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.12
	github.com/prometheus/client_golang v1.25.0
	github.com/prometheus/client_model v0.6.3
	github.com/prometheus/common v0.74.0
	github.com/sanity-io/litter v1.5.8
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.12.1
	github.com/studio-b12/gowebdav v0.13.0
	github.com/tg123/go-htpasswd v1.2.5
	github.com/zalando/go-keyring v0.2.8
	github.com/zeebo/blake3 v0.2.4
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.47.0
	go.opentelemetry.io/otel/sdk v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	go.uber.org/zap v1.28.0
	golang.org/x/crypto v0.59.0
	golang.org/x/exp v0.0.0-20261014143641-f0251eb4aa3e
	golang.org/x/mod v0.43.0
	golang.org/x/net v0.62.0
	golang.org/x/oauth2 v0.38.0
	golang.org/x/sync v0.25.0
	golang.org/x/sys v0.50.0
	golang.org/x/term v0.48.0
	golang.org/x/text v0.44.0
	golang.org/x/time v0.16.0
	google.golang.org/api v0.315.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
)

require (
	cel.dev/expr v0.26.0 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.24.4 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.1 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	cloud.google.com/go/iam v1.15.1 // indirect
	cloud.google.com/go/monitoring v1.32.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.13.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/internal v0.1.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.10.1 // indirect
	github.com/GehirnInc/crypt v0.0.0-20230320061759-8cc1b52080c5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.62.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.62.0 // indirect
	github.com/apache/arrow-go/v18 v18.9.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/danieljoos/wincred v1.2.3 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.40.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.9 // indirect
	github.com/go-json-experiment/json v1.27.2 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/goccy/go-json v0.12.0 // indirect
	github.com/godbus/dbus/v5 v5.2.4 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.25 // indirect
	github.com/googleapis/gax-go/v2 v2.27.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.2 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pandatix/go-abnf v0.5.5 // indirect
	github.com/philhofer/fwd v1.2.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/procfs v0.23.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.9.0 // indirect
	github.com/tinylib/msgp v1.6.5 // indirect
	github.com/xhit/go-str2duration/v2 v2.2.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.47.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	google.golang.org/genproto v0.0.0-20261012220534-4341edf3f941 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261104224025-d1e00e9f33ab // indirect
	gopkg.in/ini.v1 v1.68.0 // indirect
)
