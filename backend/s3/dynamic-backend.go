package s3backend

import (
	"context"
	"io/fs"
	"net/http"
	"strings"

	"github.com/rs/zerolog"
)

type FSWithCtx interface {
	http.FileSystem
	WithContext(ctx context.Context) FSWithCtx
}

type Route struct {
	Path string
	FS   FSWithCtx
}

type DynamicBackend struct {
	routes []Route
	log    zerolog.Logger
	ctx    context.Context
}

func NewDynamicBackend(log zerolog.Logger) (*DynamicBackend, error) {
	return &DynamicBackend{log: log}, nil
}

func (db *DynamicBackend) WithContext(ctx context.Context) *DynamicBackend {
	cdb := *db
	cdb.ctx = ctx
	return &cdb
}

func (db *DynamicBackend) PrependRoute(log zerolog.Logger, route Route) {
	log.Info().Str("path", route.Path).Msg("prepend route")
	db.routes = append([]Route{route}, db.routes...)
}

func (db *DynamicBackend) DeleteRoute(log zerolog.Logger, path string) *Route {
	for i, route := range db.routes {
		if path == route.Path {
			log.Info().Str("path", path).Msg("delete route")
			db.routes = append(db.routes[:i], db.routes[i+1:]...)
			return &route
		}
	}
	log.Info().Str("path", path).Msg("not found delete route")
	return nil
}

func (db *DynamicBackend) Open(name string) (http.File, error) {
	for _, route := range db.routes {
		if strings.HasPrefix(name, route.Path) {
			cfs := route.FS.WithContext(db.ctx)
			return cfs.Open(strings.TrimPrefix(name, route.Path))
		}
	}
	db.log.Warn().Str("name", name).Msg("no route found")
	return nil, fs.ErrNotExist
}
