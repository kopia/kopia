module github.com/kopia/kopia

go 1.16

require (
	cloud.google.com/go/storage v1.12.0
	contrib.go.opencensus.io/exporter/prometheus v0.2.0
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/alecthomas/kingpin v0.0.0-20200323085623-b6657d9477a6 // this is pulling master, which is newer than v2
	github.com/alecthomas/units v0.0.0-20201120081800-1786d5ef83d4 // indirect
	github.com/aws/aws-sdk-go v1.34.29
	github.com/bgentry/speakeasy v0.1.0
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/efarrer/iothrottler v0.0.1
	github.com/fatih/color v1.9.0
	github.com/foomo/htpasswd v0.0.0-20200116085101-e3a90e78da9c
	github.com/gofrs/flock v0.8.0
	github.com/golang/protobuf v1.4.2
	github.com/google/fswalker v0.2.1-0.20200214223026-f0e929ba4126
	github.com/google/go-cmp v0.5.2
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/uuid v1.1.2
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.0.4-0.20210104155004-09a3c381714c
	github.com/klauspost/compress v1.11.3
	github.com/klauspost/pgzip v1.2.5
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/minio/minio v0.0.0-20201202102351-ce0e17b62bcc
	github.com/minio/minio-go/v7 v7.0.6
	github.com/natefinch/atomic v0.0.0-20200526193002-18c0533a5b09
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/pkg/sftp v1.12.0
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/stretchr/testify v1.6.1
	github.com/studio-b12/gowebdav v0.0.0-20200929080739-bdacfab94796
	github.com/tg123/go-htpasswd v1.0.0
	github.com/zalando/go-keyring v0.1.0
	github.com/zeebo/blake3 v0.0.4
	go.opencensus.io v0.22.4
	gocloud.dev v0.20.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	golang.org/x/exp v0.0.0-20200917184745-18d7dbdd5567
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200922070232-aee5d888a860
	google.golang.org/api v0.32.0
	google.golang.org/grpc v1.32.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20191215213626-7594ed38700f
)
