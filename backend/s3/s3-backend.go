package s3backend

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/ristretto"
	"github.com/rs/zerolog"

	"github.com/mabels/diener/ctx"
)

type S3BackendImpl struct {
	bucketName      string
	maxObjectSize   int
	transferBufSize int
	maxAge          time.Duration
	svc             *s3.Client
	cache           *ristretto.Cache
	log             zerolog.Logger
	ctx             context.Context
}

func NewS3Backend(ctx ctx.AppCtx, cache *ristretto.Cache, s3Cfg ctx.S3BackendConfig) (*S3BackendImpl, error) {
	log := ctx.Log.With().Str("component", "s3-backend").Logger()
	cfg, err := config.LoadDefaultConfig(ctx.Ctx,
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: s3Cfg.Credentials,
		}))
	if err != nil {
		log.Error().Err(err).Msg("load default config")
		return nil, err
	}
	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = s3Cfg.S3.BaseEndpoint
		o.UsePathStyle = s3Cfg.S3.UsePathStyle
	})
	maxAge := time.Duration(s3Cfg.MaxAgeSeconds) * time.Second
	if maxAge == 0 {
		maxAge = time.Hour
	}
	return &S3BackendImpl{
		bucketName:      s3Cfg.BucketName,
		maxObjectSize:   s3Cfg.MaxObjectSize,
		transferBufSize: s3Cfg.TransferBufSize,
		maxAge:          maxAge,
		svc:             svc,
		cache:           cache,
		log:             ctx.Log.With().Str("component", "s3-backend").Str("bucket", s3Cfg.BucketName).Logger(),
		ctx:             ctx.Ctx,
	}, nil
}

type S3File struct {
	log     zerolog.Logger
	name    string
	obj     *s3.GetObjectOutput
	ofs     int64
	buf     []byte
	fetched time.Time
}

func (s3f *S3File) Close() error {
	s3f.log.Debug().Msg("close")
	return nil
}

func (s3f *S3File) Read(p []byte) (n int, err error) {
	s3f.log.Debug().Int64("ofs", s3f.ofs).Msg("read")
	// out := bytes.NewBuffer(p)
	chunk := s3f.obj.ContentLength - s3f.ofs
	if chunk > int64(len(p)) {
		chunk = int64(len(p))
	}
	copy(p, s3f.buf[s3f.ofs:s3f.ofs+chunk])
	// fmt.Printf("read: %d:%d:%d\n", s3f.ofs, s3f.buf[s3f.ofs], p[0])
	s3f.ofs += int64(chunk)
	return int(chunk), nil

}

func (s3f *S3File) Seek(offset int64, whence int) (int64, error) {
	if whence == 0 {
		s3f.log.Debug().Int64("ofs", offset).Msg("seek")
		s3f.ofs = offset
		return s3f.ofs, nil
	}
	s3f.log.Error().Int("whence", whence).Msg("seek not implemented")
	return 0, fs.ErrInvalid
}

func (s3f *S3File) Readdir(count int) ([]fs.FileInfo, error) {
	s3f.log.Debug().Int("count", count).Msg("readdir")
	return nil, nil
}

func (s3f *S3File) Stat() (fs.FileInfo, error) {
	s3f.log.Debug().Msg("stat")
	return &S3FileInfo{
		name: s3f.name,
		log:  s3f.log.With().Str("component", "s3-fileinfo").Logger(),
		obj:  s3f.obj,
	}, nil
}

type S3FileInfo struct {
	log  zerolog.Logger
	name string
	obj  *s3.GetObjectOutput
}

func (s3fi *S3FileInfo) Name() string {
	s3fi.log.Debug().Msg("name")
	return s3fi.name
}
func (s3fi *S3FileInfo) Size() int64 {
	s3fi.log.Debug().Int("size", int(s3fi.obj.ContentLength)).Msg("size")
	return s3fi.obj.ContentLength
}
func (s3fi *S3FileInfo) Mode() fs.FileMode {
	s3fi.log.Debug().Msg("mode")
	return 0600
}
func (s3fi *S3FileInfo) ModTime() time.Time {
	s3fi.log.Debug().Msg("modtime")
	return time.Now()
}
func (s3fi *S3FileInfo) IsDir() bool {
	s3fi.log.Debug().Msg("isdir")
	return false
}
func (s3fi *S3FileInfo) Sys() any {
	s3fi.log.Debug().Msg("sys")
	return nil

}

func (sss *S3BackendImpl) Open(name string) (http.File, error) {
	name = strings.TrimPrefix(name, "/")
	log := sss.log.With().Str("name", name).Logger()
	buf, found := sss.cache.Get(name)
	if found {
		age := time.Since(buf.(S3File).fetched)
		if age > sss.maxAge {
			log.Info().Dur("age", age).Msg("cache hit but expired")
			sss.cache.Del(name)
			found = false
		} else {
			log.Info().Int("size", len(buf.(S3File).buf)).Msg("cache hit")
			return &S3File{
				obj: buf.(S3File).obj,
				buf: buf.(S3File).buf,
			}, nil
		}
	}

	obj, err := sss.svc.GetObject(sss.ctx, &s3.GetObjectInput{
		Bucket: &sss.bucketName,
		Key:    aws.String(name),
	})
	if err != nil {
		log.Error().Err(err).Msg("get object")
		return nil, fs.ErrNotExist
	}
	if obj.ContentLength > int64(sss.maxObjectSize) {
		log.Error().Err(err).Int64("size", obj.ContentLength).Msg("max objectSize overflow")
		return nil, fs.ErrNotExist
	}
	var fileBuf bytes.Buffer
	ofs := int64(0)
	transBuf := make([]byte, sss.transferBufSize)
	for {
		rlen, err := obj.Body.Read(transBuf)
		wlen, xerr := fileBuf.Write(transBuf[:rlen])
		if xerr != nil || wlen != rlen {
			log.Error().Err(err).Int("writeLen", wlen).Int("readLen", rlen).Msg("fileBuf.Write")
			return nil, err
		}
		ofs += int64(rlen)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error().Err(err).Int64("ofs", ofs).Msg("fileBuf.Write")
			return nil, err
		}
		if rlen == 0 {
			break
		}
	}
	log = log.With().Int64("size", ofs).Logger()
	s3 := S3File{
		log:     log,
		name:    name,
		obj:     obj,
		buf:     fileBuf.Bytes(),
		fetched: time.Now(),
	}
	ret := sss.cache.Set(name, s3, int64(len(s3.buf)))
	if !ret {
		log.Warn().Msg("cache set failed")
	}
	log.Info().Msg("cache miss")
	return &s3, nil
}
