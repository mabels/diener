package ctx

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/ristretto"
	"github.com/rs/zerolog"
)

type S3BackendConfig struct {
	BucketName      string
	MaxObjectSize   int
	MaxAgeSeconds   int
	TransferBufSize int
	Credentials     aws.Credentials
	S3              s3.Options
}

type HttpConfig struct {
	Listen string
}

type Config struct {
	Ristretto  ristretto.Config
	S3Backends []S3BackendConfig
	HttpConfig HttpConfig
	// NumCounters: 1e10,    // number of keys to track frequency of (10M).
	// MaxCost:     1 << 30, // maximum cost of cache (1GB).
	// BufferItems: 64,      // number of keys per Get buffer.
}

type AppCtx struct {
	Log zerolog.Logger
	Cfg Config
	Ctx context.Context
}
