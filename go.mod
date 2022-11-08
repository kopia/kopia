module github.com/kopia/kopia

go 1.19

require (
	cloud.google.com/go/storage v1.28.0
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/GehirnInc/crypt v0.0.0-20200316065508-bb7000b8a962 // indirect
	github.com/alecthomas/kingpin v1.3.8-0.20220615105907-eae6867f4166 // this is pulling master, which is newer than v2
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a
	github.com/aws/aws-sdk-go v1.44.129
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/dustinkirkland/golang-petname v0.0.0-20191129215211-8e5a1ed0cff0
	github.com/fatih/color v1.13.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/frankban/quicktest v1.13.1 // indirect
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt/v4 v4.4.2
	github.com/golang/protobuf v1.5.2
	github.com/google/fswalker v0.2.1-0.20200214223026-f0e929ba4126
	github.com/google/go-cmp v0.5.9
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.1-0.20220112183258-f57e95bda82d
	github.com/klauspost/compress v1.15.12
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/klauspost/pgzip v1.2.5
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.13
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.43
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/natefinch/atomic v1.0.1
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.6.0
	github.com/pkg/sftp v1.13.5
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/client_golang v1.13.1
	github.com/sanity-io/litter v1.5.5
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.8.1
	github.com/studio-b12/gowebdav v0.0.0-20211106090535-29e74efa701f
	github.com/tg123/go-htpasswd v1.2.0
	github.com/zalando/go-keyring v0.2.1
	github.com/zeebo/blake3 v0.2.3
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/zap v1.23.0
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4
	golang.org/x/net v0.0.0-20221014081412-f15817d10f9b
	golang.org/x/oauth2 v0.0.0-20221014153046-6fdb5e3db783
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.0.0-20220928140112-f11e5e49a4ec
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
	golang.org/x/text v0.4.0
	google.golang.org/api v0.103.0
	google.golang.org/grpc v1.50.1
	google.golang.org/protobuf v1.28.1
	gopkg.in/ini.v1 v1.66.6 // indirect
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	cloud.google.com/go v0.105.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.8.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.3.0
	github.com/Azure/go-autorest/autorest/adal v0.9.16 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/godbus/dbus/v5 v5.0.6 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kopia/htmluibuild v0.0.0-20220928042710-9fdd02afb1e7
	github.com/kr/fs v0.1.0 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221027153422-115e99e71e1c // indirect
)

require (
	github.com/chromedp/cdproto v0.0.0-20220924210414-0e3390be1777
	github.com/chromedp/chromedp v0.8.6
	github.com/edsrzf/mmap-go v1.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/klauspost/reedsolomon v1.11.1
	go.opentelemetry.io/otel v1.11.1
	go.opentelemetry.io/otel/exporters/jaeger v1.11.0
	go.opentelemetry.io/otel/sdk v1.11.1
	go.opentelemetry.io/otel/trace v1.11.1
)

require (
	cloud.google.com/go/compute v1.12.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.1 // indirect
	cloud.google.com/go/iam v0.6.0 // indirect
	github.com/alessio/shellescape v1.4.1 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.1.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/xhit/go-str2duration v1.2.0 // indirect
)
