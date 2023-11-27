package k8sinformers

import (
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/ristretto"
	s3backend "github.com/mabels/diener/backend/s3"
	"github.com/mabels/diener/ctx"
	k8scrds "github.com/mabels/diener/k8s/crds"
	"github.com/rs/zerolog"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	informernetv1 "k8s.io/client-go/informers/networking/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// import (
// 	"reflect"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/dgraph-io/ristretto"
// 	"github.com/mabels/diener/ctx"
// 	k8scrds "github.com/mabels/diener/k8s/crds"
// 	"github.com/rs/zerolog"
// 	"github.com/rs/zerolog/log"
// 	"k8s.io/client-go/tools/cache"
// )

type ingressHandler struct {
	stopCh         chan struct{}
	informer       cache.SharedIndexInformer
	indexers       map[string]cache.IndexFunc
	namespace      string
	appCtx         ctx.AppCtx
	log            zerolog.Logger
	dienerApi      k8scrds.DienerV1Alpha1Interface
	rcache         *ristretto.Cache
	dynamicBackend *s3backend.DynamicBackend
}

func getPaths(ingress *netv1.Ingress) []netv1.HTTPIngressPath {
	paths := []netv1.HTTPIngressPath{}
	for _, rule := range ingress.Spec.Rules {
		paths = append(paths, rule.HTTP.Paths...)
		// for _, path := range rule.HTTP.Paths {
		// 	paths = append(paths, path)
		// }
	}
	return paths
}

func (ih ingressHandler) AddFunc(obj interface{}) {

}

// func (ih IngressHandler) UpdateFunc(oldObj, newObj interface{}) {

// func (ih IngressHandler) DeleteFunc(obj interface{}) {

// }

// var pathHandlers = map[string]pathHandler{}
// var pathHandlerMutex = sync.Mutex{}

func (ih ingressHandler) OnAdd(obj interface{}, isInInitialList bool) {
	log := ih.log
	ingress, found := obj.(*netv1.Ingress)
	if !found {
		log.Error().Msg("ingress not found")
		return
	}
	log = log.With().Str("name", ingress.Name).Str("uid", string(ingress.UID)).Logger()
	for _, path := range getPaths(ingress) {
		if path.Backend.Resource != nil {
			if path.Backend.Resource.APIGroup != nil && *path.Backend.Resource.APIGroup != "diener.adviser.com" {
				continue
			}
			if path.Backend.Resource.Kind != "S3Backend" {
				continue
			}
			s3b, err := ih.dienerApi.S3Backends(ingress.Namespace).Get(path.Backend.Resource.Name, metav1.GetOptions{})
			log.Debug().Any("obj", s3b).Any("resource", path.Backend.Resource).Msg("select")
			if err != nil {
				log.Error().Err(err).Msg("get s3 backend")
				continue
			}
			region := "not-set"
			if s3b.Spec.Region != nil {
				region = *s3b.Spec.Region
			}
			fs, err := s3backend.NewS3Backend(ih.appCtx, ih.rcache, ctx.S3BackendConfig{
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
					Region:       region,
				},
			})
			if err != nil {
				log.Error().Err(err).Msg("new s3 backend")
				return
			}
			ih.dynamicBackend.PrependRoute(log, s3backend.Route{
				Path: path.Path,
				FS:   fs,
			})
		}

	}
}

func (ih ingressHandler) OnUpdate(oldObj, newObj interface{}) {
	oldIngress, oldFound := oldObj.(*netv1.Ingress)
	newIngress, newFound := oldObj.(*netv1.Ingress)
	if oldFound && newFound && !reflect.DeepEqual(oldIngress.Spec, newIngress.Spec) {
		ih.OnDelete(oldObj)
		ih.OnAdd(newObj, false)
	}
}
func (ih ingressHandler) OnDelete(obj interface{}) {
	ingress, found := obj.(*netv1.Ingress)
	if found {
		log := ih.log.With().Str("name", ingress.Name).Str("uid", string(ingress.UID)).Logger()
		for _, path := range getPaths(ingress) {
			log = log.With().Str("path", path.Path).Logger()
			ih.dynamicBackend.DeleteRoute(log, path.Path)
		}
	}
}

var ingressInformers = map[string]ingressHandler{}

var ingressMutex = sync.Mutex{}

func NewIngressHandlerByNamespace(ns string, appCtx ctx.AppCtx, config *rest.Config, dynamicBackend *s3backend.DynamicBackend, dienerApi k8scrds.DienerV1Alpha1Interface, rcache *ristretto.Cache) {
	log := appCtx.Log.With().Str("namespace", ns).Str("component", "ingress-handler").Logger()
	ingressMutex.Lock()
	defer ingressMutex.Unlock()
	_, found := ingressInformers[ns]
	if found {
		log.Error().Msg("ingress handler already exists")
		return
	}

	kif, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error().Err(err).Msg("new for config")
		return
	}
	indexers := map[string]cache.IndexFunc{}
	informer := informernetv1.NewIngressInformer(kif, ns, time.Minute, indexers)
	ih := ingressHandler{
		stopCh:         make(chan struct{}, 1),
		indexers:       indexers,
		informer:       informer,
		namespace:      ns,
		appCtx:         appCtx,
		rcache:         rcache,
		dynamicBackend: dynamicBackend,
		log:            log,
		dienerApi:      dienerApi,
	}
	informer.AddEventHandler(ih)

	ingressInformers[ns] = ih

	go informer.Run(ih.stopCh)

	ih.log.Info().Msg("started ingress informer")
}

func DeleteIngressHandlerByNamespace(ns string, appCtx ctx.AppCtx) {
	ih, found := ingressInformers[ns]
	if !found {
		appCtx.Log.Warn().Str("namespace", ns).Msg("ingress handler does not exist")
		return
	}
	ih.stopCh <- struct{}{}
	delete(ingressInformers, ns)
	ih.log.Info().Msg("delete ingress informer")
}
