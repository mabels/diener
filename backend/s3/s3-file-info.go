package s3backend

import (
	"context"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

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
