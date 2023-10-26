package s3backend

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/rs/zerolog"
)

type Route struct {
	Path string
	FS   http.FileSystem
}

type DynamicBackend struct {
	routes []Route
	log    zerolog.Logger
}

func NewDynamicBackend(log zerolog.Logger) (*DynamicBackend, error) {
	return &DynamicBackend{log: log}, nil
}

func (db *DynamicBackend) PrependRoute(route Route) {
	db.log.Info().Str("path", route.Path).Msg("prepend route")
	db.routes = append([]Route{route}, db.routes...)
}

func (db *DynamicBackend) Open(name string) (http.File, error) {
	for _, route := range db.routes {
		if strings.HasPrefix(name, route.Path) {
			return route.FS.Open(strings.TrimPrefix(name, route.Path))
		}
	}
	db.log.Warn().Str("name", name).Msg("no route found")
	return nil, fs.ErrNotExist
}
