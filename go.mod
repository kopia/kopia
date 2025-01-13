module github.com/kopia/kopia

go 1.22.7

require (
	cloud.google.com/go/storage v1.50.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.17.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.8.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.5.0
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/chromedp/cdproto v0.0.0-20241003230502-a4a8f7c660df
	github.com/chromedp/chromedp v0.11.0
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/dustinkirkland/golang-petname v0.0.0-20191129215211-8e5a1ed0cff0
	github.com/edsrzf/mmap-go v1.2.0
	github.com/fatih/color v1.18.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/gofrs/flock v0.12.1
	github.com/golang-jwt/jwt/v4 v4.5.1
	github.com/google/fswalker v0.3.3
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hanwen/go-fuse/v2 v2.7.2
	github.com/hashicorp/cronexpr v1.1.2
	github.com/klauspost/compress v1.17.11
	github.com/klauspost/pgzip v1.2.6
	github.com/klauspost/reedsolomon v1.12.4
	github.com/kopia/htmluibuild v0.0.1-0.20241228091102-21c41d704c1b
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.13
	github.com/mattn/go-isatty v0.0.20
	github.com/minio/minio-go/v7 v7.0.83
	github.com/mocktools/go-smtp-mock/v2 v2.4.0
	github.com/mxk/go-vss v1.2.0
	github.com/natefinch/atomic v1.0.1
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.7.0
	github.com/pkg/sftp v1.13.7
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common v0.61.0
	github.com/sanity-io/litter v1.5.5
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.10.0
	github.com/studio-b12/gowebdav v0.10.0
	github.com/tg123/go-htpasswd v1.2.3
	github.com/zalando/go-keyring v0.2.6
	github.com/zeebo/blake3 v0.2.4
	go.opentelemetry.io/otel v1.33.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.33.0
	go.opentelemetry.io/otel/sdk v1.33.0
	go.opentelemetry.io/otel/trace v1.33.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.32.0
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/mod v0.22.0
	golang.org/x/net v0.34.0
	golang.org/x/oauth2 v0.25.0
	golang.org/x/sync v0.10.0
	golang.org/x/sys v0.29.0
	golang.org/x/term v0.28.0
	golang.org/x/text v0.21.0
	google.golang.org/api v0.215.0
	google.golang.org/grpc v1.68.1
	google.golang.org/protobuf v1.36.2
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
)

require (
	al.essio.dev/pkg/shellescape v1.5.1 // indirect
	cel.dev/expr v0.16.1 // indirect
	cloud.google.com/go v0.116.0 // indirect
	cloud.google.com/go/auth v0.13.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.6 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.2.2 // indirect
	cloud.google.com/go/monitoring v1.21.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/GehirnInc/crypt v0.0.0-20230320061759-8cc1b52080c5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.25.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.48.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.48.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/cncf/xds/go v0.0.0-20240905190251-b4127c9b8d78 // indirect
	github.com/danieljoos/wincred v1.2.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/go-control-plane v0.13.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.1.0 // indirect
	github.com/felixge/fgprof v0.9.3 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/frankban/quicktest v1.13.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	github.com/goccy/go-json v0.10.4 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang/glog v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/pprof v0.0.0-20230602150820-91b7bce49751 // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.24.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.29.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.54.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.54.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.33.0 // indirect
	go.opentelemetry.io/otel/metric v1.33.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.29.0 // indirect
	go.opentelemetry.io/proto/otlp v1.4.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20241118233622-e639e219e697 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241209162323-e6fa225c2576 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241223144023-3abc09e42ca8 // indirect
	google.golang.org/grpc/stats/opentelemetry v0.0.0-20240907200651-3ffb98b2c93a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
