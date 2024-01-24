package s3backend

import (
	"context"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type S3CachedFile struct {
	log     zerolog.Logger
	tracer  trace.Tracer
	ctx     context.Context
	name    string
	obj     *s3.GetObjectOutput
	ofs     int64
	buf     []byte
	fetched time.Time
}

func (s3f *S3CachedFile) Close() error {
	_, trace := s3f.tracer.Start(s3f.ctx, "close")
	defer trace.End()
	trace.AddEvent(s3f.name)
	s3f.log.Debug().Msg("close")
	return nil
}

func (s3f *S3CachedFile) Read(p []byte) (n int, err error) {
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

func (s3f *S3CachedFile) Seek(offset int64, whence int) (int64, error) {
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

func (s3f *S3CachedFile) Readdir(count int) ([]fs.FileInfo, error) {
	_, trace := s3f.tracer.Start(s3f.ctx, "readdir")
	defer trace.End()
	trace.AddEvent(s3f.name)
	trace.SetAttributes(attribute.Int("count", count))
	s3f.log.Debug().Int("count", count).Msg("readdir")
	return nil, nil
}

func (s3f *S3CachedFile) Stat() (fs.FileInfo, error) {
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
