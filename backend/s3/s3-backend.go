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

func (sss *S3BackendImpl) Open(name string) (http.File, error) {
	octx, span := sss.tracer.Start(sss.ctx, "Open")
	defer span.End()
	span.AddEvent(name)

	name = strings.TrimPrefix(name, "/")
	log := sss.log.With().Str("name", name).Logger()
	buf, found := sss.cache.Get(name)
	if found {
		age := time.Since(buf.(S3CachedFile).fetched)
		span.SetAttributes(attribute.Int("size", len(buf.(S3CachedFile).buf)))
		span.SetAttributes(attribute.Int64("age", int64(age)))
		if age > sss.maxAge {
			span.SetStatus(otelcodes.Ok, "cache hit but expired")
			log.Info().Dur("age", age).Msg("cache hit but expired")
			sss.cache.Del(name)
			found = false
		} else {
			span.SetStatus(otelcodes.Ok, "cache hit")
			log.Info().Int("size", len(buf.(S3CachedFile).buf)).Msg("cache hit")
			ifile := buf.(S3CachedFile)
			return &S3CachedFile{
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
		log.Warn().Int64("size", obj.ContentLength).Msg("max objectSize overflow")
		return &S3DirectFile{
			log:     log,
			tracer:  sss.tracer,
			ctx:     octx,
			name:    name,
			obj:     obj,
			fetched: time.Now(),
		}, nil
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
	s3 := S3CachedFile{
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
