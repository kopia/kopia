module github.com/kopia/kopia

go 1.22

require (
	cloud.google.com/go/storage v1.40.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.5.1
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.3.2
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/chromedp/cdproto v0.0.0-20240202021202-6d0b6a386732
	github.com/chromedp/chromedp v0.9.5
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/dustinkirkland/golang-petname v0.0.0-20191129215211-8e5a1ed0cff0
	github.com/edsrzf/mmap-go v1.1.0
	github.com/fatih/color v1.16.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/google/fswalker v0.3.3
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hanwen/go-fuse/v2 v2.5.1
	github.com/hashicorp/cronexpr v1.1.2
	github.com/klauspost/compress v1.17.8
	github.com/klauspost/pgzip v1.2.6
	github.com/klauspost/reedsolomon v1.12.1
	github.com/kopia/htmluibuild v0.0.1-0.20240405040759-90bfbf000696
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.13
	github.com/minio/minio-go/v7 v7.0.69
	github.com/mxk/go-vss v1.2.0
	github.com/natefinch/atomic v1.0.1
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.7.0
	github.com/pkg/sftp v1.13.6
	github.com/prometheus/client_golang v1.19.0
	github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common v0.52.2
	github.com/sanity-io/litter v1.5.5
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.9.0
	github.com/studio-b12/gowebdav v0.9.0
	github.com/tg123/go-htpasswd v1.2.2
	github.com/zalando/go-keyring v0.2.4
	github.com/zeebo/blake3 v0.2.3
	go.opentelemetry.io/otel v1.25.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.25.0
	go.opentelemetry.io/otel/sdk v1.25.0
	go.opentelemetry.io/otel/trace v1.25.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.22.0
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/mod v0.17.0
	golang.org/x/net v0.24.0
	golang.org/x/oauth2 v0.19.0
	golang.org/x/sync v0.7.0
	golang.org/x/sys v0.19.0
	golang.org/x/term v0.19.0
	golang.org/x/text v0.14.0
	google.golang.org/api v0.172.0
	google.golang.org/grpc v1.63.2
	google.golang.org/protobuf v1.33.0
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
)

require (
	cloud.google.com/go v0.112.1 // indirect
	cloud.google.com/go/compute v1.24.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.7 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.2 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.1 // indirect
	github.com/GehirnInc/crypt v0.0.0-20230320061759-8cc1b52080c5 // indirect
	github.com/alessio/shellescape v1.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/danieljoos/wincred v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/felixge/fgprof v0.9.3 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/frankban/quicktest v1.13.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.3.2 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.0 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/pprof v0.0.0-20230602150820-91b7bce49751 // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.6 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.25.0 // indirect
	go.opentelemetry.io/otel/metric v1.25.0 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240314234333-6e1732d8331c // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
