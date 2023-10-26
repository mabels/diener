package main

import (
	"context"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3backend "github.com/mabels/diener/backend/s3"
	"github.com/mabels/diener/ctx"
	k8scrds "github.com/mabels/diener/k8s/crds"
	k8sinformers "github.com/mabels/diener/k8s/informers"

	"github.com/rs/zerolog"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/spf13/pflag"

	"github.com/dgraph-io/ristretto"
)

func main() {
	var kubeconfig string
	pflag.StringVar(&kubeconfig, "kubeconfig", "", "path to Kubernetes config file")
	var listen string
	pflag.StringVar(&listen, "listen", ":8282", "listen address")
	pflag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	appCtx := ctx.AppCtx{
		Log: log,
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
		Ctx: context.Background(),
	}
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

	informer, err := k8sinformers.RootInformer(config, log, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ingress, found := obj.(*netv1.Ingress)
			if found {
				log = log.With().Str("namespace", ingress.Namespace).Str("name", ingress.Name).Logger()
				for _, rule := range ingress.Spec.Rules {
					for _, path := range rule.HTTP.Paths {
						if path.Backend.Resource != nil {
							if path.Backend.Resource.APIGroup != nil && *path.Backend.Resource.APIGroup != "diener.adviser.com" {
								continue
							}
							if path.Backend.Resource.Kind != "S3Backend" {
								continue
							}
							s3b, err := dienerApi.S3Backends(ingress.Namespace).Get(path.Backend.Resource.Name, metav1.GetOptions{})
							log.Debug().Any("obj", s3b).Any("resource", path.Backend.Resource).Msg("select")
							if err != nil {
								log.Error().Err(err).Msg("get s3 backend")
								continue
							}
							fs, err := s3backend.NewS3Backend(appCtx, rcache, ctx.S3BackendConfig{
								BucketName:      s3b.Spec.BucketName,
								MaxObjectSize:   s3b.Spec.MaxObjectSize,
								TransferBufSize: s3b.Spec.TransferBufSize,
								MaxAgeSeconds:   s3b.Spec.MaxAgeSeconds,
								Credentials: aws.Credentials{
									AccessKeyID:     s3b.Spec.AccessKey,
									SecretAccessKey: s3b.Spec.SecretKey,
								},
								S3: s3.Options{
									BaseEndpoint: s3b.Spec.Endpoint,
									UsePathStyle: true,
								},
							})
							if err != nil {
								log.Error().Err(err).Msg("new s3 backend")
								return
							}
							dynamicBackend.PrependRoute(s3backend.Route{
								Path: path.Path,
								FS:   fs,
							})
						}
					}
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {},
		DeleteFunc: func(obj interface{}) {},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create root informer")
	}
	go informer.Run(wait.NeverStop)

	log.Debug().Str("listen", appCtx.Cfg.HttpConfig.Listen).Msg("starting server")
	err = http.ListenAndServe(appCtx.Cfg.HttpConfig.Listen, http.FileServer(dynamicBackend))
	if err != nil {
		log.Fatal().Err(err).Msg("listen and serve")
	}
}
