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
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type S3BackendImpl struct {
	bucketName      string
	maxObjectSize   int
	transferBufSize int
	maxAge          time.Duration
	svc             *s3.Client
	cache           *ristretto.Cache
	log             zerolog.Logger
	tracer          trace.Tracer
	ctx             context.Context
}

func (sss *S3BackendImpl) WithContext(ctx context.Context) FSWithCtx {
	csss := *sss
	csss.ctx = ctx
	return &csss
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
		o.Region = s3Cfg.S3.Region
	})
	log.Debug().Any("s3Cfg", s3Cfg).Msg("new s3 backend")
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
		// span:            trace,
		tracer: ctx.Tracer,
		ctx:    ctx.Ctx,
	}, nil
}

type S3File struct {
	log     zerolog.Logger
	tracer  trace.Tracer
	ctx     context.Context
	name    string
	obj     *s3.GetObjectOutput
	ofs     int64
	buf     []byte
	fetched time.Time
}

func (s3f *S3File) Close() error {
	_, trace := s3f.tracer.Start(s3f.ctx, "close")
	defer trace.End()
	trace.AddEvent(s3f.name)
	s3f.log.Debug().Msg("close")
	return nil
}

func (s3f *S3File) Read(p []byte) (n int, err error) {
	_, trace := s3f.tracer.Start(s3f.ctx, "read")
	defer trace.End()
	trace.AddEvent(s3f.name)
	trace.SetAttributes(attribute.Int64("ofs", s3f.ofs))
	// out := bytes.NewBuffer(p)
	chunk := s3f.obj.ContentLength - s3f.ofs
	if chunk > int64(len(p)) {
		chunk = int64(len(p))
	}
	copy(p, s3f.buf[s3f.ofs:s3f.ofs+chunk])
	// fmt.Printf("read: %d:%d:%d\n", s3f.ofs, s3f.buf[s3f.ofs], p[0])
	s3f.ofs += int64(chunk)
	trace.SetAttributes(attribute.Int64("len", chunk))
	return int(chunk), nil

}

func (s3f *S3File) Seek(offset int64, whence int) (int64, error) {
	_, trace := s3f.tracer.Start(s3f.ctx, "seek")
	defer trace.End()
	trace.AddEvent(s3f.name)
	trace.SetAttributes(attribute.Int("whence", whence))
	if whence == 0 {
		s3f.log.Debug().Int64("ofs", offset).Msg("seek")
		trace.SetAttributes(attribute.Int64("ofs", offset))
		s3f.ofs = offset
		return s3f.ofs, nil
	}
	trace.SetStatus(otelcodes.Error, "seek not implemented")
	s3f.log.Error().Int("whence", whence).Msg("seek not implemented")
	return 0, fs.ErrInvalid
}

func (s3f *S3File) Readdir(count int) ([]fs.FileInfo, error) {
	_, trace := s3f.tracer.Start(s3f.ctx, "readdir")
	defer trace.End()
	trace.AddEvent(s3f.name)
	trace.SetAttributes(attribute.Int("count", count))
	s3f.log.Debug().Int("count", count).Msg("readdir")
	return nil, nil
}

func (s3f *S3File) Stat() (fs.FileInfo, error) {
	octx, trace := s3f.tracer.Start(s3f.ctx, "stat")
	defer trace.End()
	trace.AddEvent(s3f.name)
	s3f.log.Debug().Msg("stat")
	return &S3FileInfo{
		name:   s3f.name,
		ctx:    octx,
		tracer: s3f.tracer,
		log:    s3f.log.With().Str("component", "s3-fileinfo").Logger(),
		obj:    s3f.obj,
		time:   s3f.fetched,
	}, nil
}

type S3FileInfo struct {
	log    zerolog.Logger
	tracer trace.Tracer
	ctx    context.Context
	name   string
	obj    *s3.GetObjectOutput
	time   time.Time
}

func (s3fi *S3FileInfo) Name() string {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "Name")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	s3fi.log.Debug().Msg("name")
	return s3fi.name
}
func (s3fi *S3FileInfo) Size() int64 {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "Size")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	trace.SetAttributes(attribute.Int64("size", int64(s3fi.obj.ContentLength)))
	s3fi.log.Debug().Int("size", int(s3fi.obj.ContentLength)).Msg("size")
	return s3fi.obj.ContentLength
}
func (s3fi *S3FileInfo) Mode() fs.FileMode {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "Mode")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	s3fi.log.Debug().Msg("mode")
	return 0600
}
func (s3fi *S3FileInfo) ModTime() time.Time {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "ModTime")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	s3fi.log.Debug().Msg("modtime")
	return s3fi.time
}
func (s3fi *S3FileInfo) IsDir() bool {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "IsDir")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	s3fi.log.Debug().Msg("isdir")
	return false
}
func (s3fi *S3FileInfo) Sys() any {
	_, trace := s3fi.tracer.Start(s3fi.ctx, "Sys")
	defer trace.End()
	trace.AddEvent(s3fi.name)
	s3fi.log.Debug().Msg("sys")
	return nil

}

func (sss *S3BackendImpl) Open(name string) (http.File, error) {
	octx, span := sss.tracer.Start(sss.ctx, "Open")
	defer span.End()
	span.AddEvent(name)

	name = strings.TrimPrefix(name, "/")
	log := sss.log.With().Str("name", name).Logger()
	buf, found := sss.cache.Get(name)
	if found {
		age := time.Since(buf.(S3File).fetched)
		span.SetAttributes(attribute.Int("size", len(buf.(S3File).buf)))
		span.SetAttributes(attribute.Int64("age", int64(age)))
		if age > sss.maxAge {
			span.SetStatus(otelcodes.Ok, "cache hit but expired")
			log.Info().Dur("age", age).Msg("cache hit but expired")
			sss.cache.Del(name)
			found = false
		} else {
			span.SetStatus(otelcodes.Ok, "cache hit")
			log.Info().Int("size", len(buf.(S3File).buf)).Msg("cache hit")
			ifile := buf.(S3File)
			return &S3File{
				log:     ifile.log,
				tracer:  sss.tracer,
				ctx:     octx,
				name:    ifile.name,
				obj:     ifile.obj,
				ofs:     0,
				buf:     ifile.buf,
				fetched: ifile.fetched,
			}, nil
		}
	}

	span.SetStatus(otelcodes.Ok, "cache miss")
	span.SetAttributes(attribute.String("bucket", sss.bucketName))
	span.SetAttributes(attribute.String("name", name))
	obj, err := sss.svc.GetObject(sss.ctx, &s3.GetObjectInput{
		Bucket: &sss.bucketName,
		Key:    aws.String(name),
	})
	if err != nil {
		span.SetStatus(otelcodes.Error, err.Error())
		log.Error().Err(err).Msg("get object")
		return nil, fs.ErrNotExist
	}
	span.SetAttributes(attribute.Int64("size", obj.ContentLength))
	if obj.ContentLength > int64(sss.maxObjectSize) {
		span.SetStatus(otelcodes.Error, "max object size overflow")
		log.Error().Err(err).Int64("size", obj.ContentLength).Msg("max objectSize overflow")
		return nil, fs.ErrNotExist
	}
	var fileBuf bytes.Buffer
	ofs := int64(0)
	transBuf := make([]byte, sss.transferBufSize)
	for {
		rlen, err := obj.Body.Read(transBuf)
		span.SetAttributes(attribute.Int64("rLen", int64(rlen)))
		wlen, werr := fileBuf.Write(transBuf[:rlen])
		span.SetAttributes(attribute.Int64("wlen", int64(wlen)))
		if werr != nil || wlen != rlen {
			if werr != nil {
				span.SetStatus(otelcodes.Error, werr.Error())
			} else {
				span.SetStatus(otelcodes.Error, "wlen != rlen")
			}
			log.Error().Err(err).Int("writeLen", wlen).Int("readLen", rlen).Msg("fileBuf.Write")
			return nil, err
		}
		ofs += int64(rlen)
		if err != nil {
			if err == io.EOF {
				span.SetStatus(otelcodes.Ok, "eof")
				break
			}
			span.SetStatus(otelcodes.Error, err.Error())
			log.Error().Err(err).Int64("ofs", ofs).Msg("fileBuf.Write")
			return nil, err
		}
		if rlen == 0 {
			span.SetStatus(otelcodes.Ok, "zero read")
			break
		}
	}
	span.SetAttributes(attribute.Int64("size", ofs))
	log = log.With().Int64("size", ofs).Logger()
	s3 := S3File{
		log:     log,
		tracer:  sss.tracer,
		ctx:     octx,
		name:    name,
		obj:     obj,
		buf:     fileBuf.Bytes(),
		fetched: time.Now(),
	}
	ret := sss.cache.Set(name, s3, int64(len(s3.buf)))
	if !ret {
		span.SetStatus(otelcodes.Error, "cache set failed")
		log.Warn().Msg("cache set failed")
	}
	span.SetStatus(otelcodes.Ok, "cache miss")
	log.Info().Msg("cache miss")
	return &s3, nil
}
