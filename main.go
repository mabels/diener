package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"time"

	s3backend "github.com/mabels/diener/backend/s3"
	"github.com/mabels/diener/ctx"
	k8scrds "github.com/mabels/diener/k8s/crds"
	k8sinformers "github.com/mabels/diener/k8s/informers"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"

	"github.com/rs/zerolog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/spf13/pflag"

	"github.com/dgraph-io/ristretto"
)

// var resource *sdkresource.Resource
// var initResourcesOnce sync.Once

// var log *logrus.Logger

// func init() {
// 	log = logrus.New()
// 	log.Level = logrus.DebugLevel
// 	log.Formatter = &logrus.JSONFormatter{
// 		FieldMap: logrus.FieldMap{
// 			logrus.FieldKeyTime:  "timestamp",
// 			logrus.FieldKeyLevel: "severity",
// 			logrus.FieldKeyMsg:   "message",
// 		},
// 		TimestampFormat: time.RFC3339Nano,
// 	}
// 	log.Out = os.Stdout
// }

// func initResource() *sdkresource.Resource {
// 	initResourcesOnce.Do(func() {
// 		extraResources, _ := sdkresource.New(
// 			context.Background(),
// 			sdkresource.WithOS(),
// 			sdkresource.WithProcess(),
// 			sdkresource.WithContainer(),
// 			sdkresource.WithHost(),
// 		)
// 		resource, _ = sdkresource.Merge(
// 			sdkresource.Default(),
// 			extraResources,
// 		)
// 	})
// 	return resource
// }

// func initTracerProvider() (*sdktrace.TracerProvider, error) {
// 	ctx := context.Background()

// 	exporter, err := otlptracegrpc.New(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	tp := sdktrace.NewTracerProvider(
//                 sdktrace.WithSampler(sdktrace.AlwaysSample()),
// 		sdktrace.WithBatcher(exporter),
// 		sdktrace.WithResource(initResource()),
// 	)
// 	otel.SetTracerProvider(tp)
// 	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
// 	return tp, nil
// }

// func initMeterProvider() *sdkmetric.MeterProvider {
// 	ctx := context.Background()

// 	exporter, err := otlpmetricgrpc.New(ctx)
// 	if err != nil {
// 		log.Fatalf("new otlp metric grpc exporter failed: %v", err)
// 	}

// 	mp := sdkmetric.NewMeterProvider(
// 		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
// 		sdkmetric.WithResource(initResource()),
// 		sdkmetric.WithView(sdkmetric.NewView(
// 			sdkmetric.Instrument{Scope: instrumentation.Scope{Name: "go.opentelemetry.io/contrib/google.golang.org/grpc/otelgrpc"}},
// 			sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}},
// 		)),
// 	)
// 	otel.SetMeterProvider(mp)
// 	return mp
// }

func main() {

	var kubeconfig string
	pflag.StringVar(&kubeconfig, "kubeconfig", "", "path to Kubernetes config file")
	var listen string
	pflag.StringVar(&listen, "listen", ":8282", "listen address")
	var debug bool
	pflag.BoolVar(&debug, "debug", false, "set debug")
	pflag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// tp, err := initTracerProvider()
	// if err != nil {
	// 	log.Error().Err(err).Msg("init tracer provider")
	// 	return
	// }
	// defer func() {
	// 	if err := tp.Shutdown(context.Background()); err != nil {
	// 		log.Error().Err(err).Msg("shutdown tracer provider")
	// 		return
	// 	}
	// }()

	// mp := initMeterProvider()
	// defer func() {
	// 	if err := mp.Shutdown(context.Background()); err != nil {
	// 		log.Printf("Error shutting down meter provider: %v", err)
	// 	}
	// }()

	octx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Set up OpenTelemetry.
	serviceName := "diener"
	serviceVersion := "0.1.0"
	otelShutdown, err := setupOTelSDK(octx, serviceName, serviceVersion)
	if err != nil {
		log.Error().Err(err).Msg("setupOTelSDK")
		return
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	appCtx := ctx.AppCtx{
		Log:    log,
		Tracer: otel.Tracer("diener"),
		Meter:  otel.Meter("diener"),
		Cfg: ctx.Config{
			HttpConfig: ctx.HttpConfig{
				Listen: listen,
			},

			Ristretto: ristretto.Config{
				NumCounters: 1e10,    // number of keys to track frequency of (10M).
				MaxCost:     1 << 30, // maximum cost of cache (1GB).
				BufferItems: 64,      // number of keys per Get buffer
			},
		},
		Ctx: octx,
	}

	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		log.Error().Err(err).Msg("start runtime")
		return
	}

	_, span := appCtx.Tracer.Start(appCtx.Ctx, "main")
	defer span.End()

	rcache, err := ristretto.NewCache(&appCtx.Cfg.Ristretto)
	if err != nil {
		log.Error().Err(err).Msg("new cache")
		return
	}

	dynamicBackend, err := s3backend.NewDynamicBackend(appCtx.Log)
	if err != nil {
		log.Error().Err(err).Msg("new cache")
		return
	}

	var config *rest.Config
	log = log.With().Str("component", "root-informer").Logger()
	if kubeconfig == "" {
		log.Info().Msg("using in-cluster configuration")
		config, err = rest.InClusterConfig()
	} else {
		log.Info().Str("file", kubeconfig).Msg("using configuration from")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		log.Error().Err(err).Msg("failed to create configuration")
		return
	}

	k8scrds.AddToScheme(scheme.Scheme)
	dienerApi, err := k8scrds.NewForConfig(appCtx.Ctx, config)
	if err != nil {
		log.Error().Err(err).Msg("new for config")
		return
	}

	// handlers := []cache.ResourceEventHandlerFuncs{
	// 	// k8sinformers.NewIngressHandler(appCtx, dynamicBackend, dienerApi, rcache),
	// 	// k8sinformers.NewNamespacesHandler(appCtx),
	// 	// k8sinformers.NewS3BackendHandler(appCtx),
	// }

	informer, err := k8sinformers.NamespacedInformer(config, dienerApi, log, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ns, ok := obj.(*v1.Namespace)
			if !ok {
				log.Warn().Str("func", "AddFunc").Str("type", reflect.TypeOf(obj).Name()).Msg("not a namespace")
				return
			}
			k8sinformers.NewIngressHandlerByNamespace(ns.Name, appCtx, config, dynamicBackend, dienerApi, rcache)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			_, oldOk := oldObj.(*v1.Namespace)
			_, newOk := newObj.(*v1.Namespace)
			if !oldOk || !newOk {
				log.Warn().Str("func", "UpdateFunc").
					Str("oldType", reflect.TypeOf(oldObj).Name()).
					Str("newType", reflect.TypeOf(newObj).Name()).
					Msg("not a namespace")
				return
			}
			// fmt.Printf("update %v => %v\n", oldNs.Name, newNs.Name)
		},
		DeleteFunc: func(obj interface{}) {
			ns, ok := obj.(*v1.Namespace)
			if !ok {
				log.Warn().Str("func", "DeleteFunc").
					Str("type", reflect.TypeOf(obj).Name()).
					Msg("not a namespace")
				return
			}
			fmt.Printf("delete %v\n", ns.Name)
			k8sinformers.DeleteIngressHandlerByNamespace(ns.Name, appCtx)
		},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create root informer")
	}
	go informer.Run(wait.NeverStop)

	log.Debug().Str("listen", appCtx.Cfg.HttpConfig.Listen).Msg("starting server")

	srv := &http.Server{
		Addr:         appCtx.Cfg.HttpConfig.Listen,
		BaseContext:  func(_ net.Listener) context.Context { return octx },
		ReadTimeout:  time.Second,
		WriteTimeout: 10 * time.Second,
		Handler:      newHTTPHandler(appCtx, dynamicBackend),
	}
	srvErr := make(chan error, 1)
	go func() {
		srvErr <- srv.ListenAndServe()
	}()

	// Wait for interruption.
	select {
	case err = <-srvErr:
		// Error when starting HTTP server.
		return
	case <-octx.Done():
		// Wait for first CTRL+C.
		// Stop receiving signal notifications as soon as possible.
		stop()
	}

	// When Shutdown is called, ListenAndServe immediately returns ErrServerClosed.
	err = srv.Shutdown(context.Background())

	// err = http.ListenAndServe(appCtx.Cfg.HttpConfig.Listen, http.FileServer(dynamicBackend))
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("listen and serve")
	// }
}

func newHTTPHandler(appCtx ctx.AppCtx, db *s3backend.DynamicBackend) http.Handler {
	hdl := func(w http.ResponseWriter, r *http.Request) {
		// ctx, span := appCtx.Tracer.Start(r.Context(), r.URL.Path)
		// defer span.End()
		// _, hspan := appCtx.Tracer.Start(r.Context(), "reqHeader")
		// for k, v := range r.Header {
		// 	span.SetAttributes(attribute.String(k, v[0]))
		// }
		// hspan.End()
		otelhandler := otelhttp.WithRouteTag(r.URL.Path, http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
			ctx, span := appCtx.Tracer.Start(r.Context(), r.URL.Path)
			defer span.End()
			cdb := db.WithContext(ctx)
			http.FileServer(cdb).ServeHTTP(wr, req.WithContext(ctx))
		}))
		otelhandler.ServeHTTP(w, r)
	}

	// Add HTTP instrumentation for the whole server.
	handler := otelhttp.NewHandler(http.HandlerFunc(hdl), "ingress")
	return handler
}
