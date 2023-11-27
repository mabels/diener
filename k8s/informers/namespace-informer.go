package k8sinformers

import (
	"time"

	k8scrds "github.com/mabels/diener/k8s/crds"
	"github.com/rs/zerolog"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	informercorev1 "k8s.io/client-go/informers/core/v1"

	kubernetes "k8s.io/client-go/kubernetes"
)

// type subInformer struct {
// }

// type subInformers struct {
// 	subInformers map[string]subInformer
// }

// func newSubInformers() *subInformers {
// 	return &subInformers{
// 		subInformers: map[string]subInformer{},
// 	}
// }

func NamespacedInformer(config *rest.Config, dienerApi k8scrds.DienerV1Alpha1Interface, log zerolog.Logger, handler cache.ResourceEventHandlerFuncs) (cache.SharedIndexInformer, error) {
	kif, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error().Err(err).Msg("new for config")
		return nil, err
	}
	indexers := map[string]cache.IndexFunc{}
	nsInformer := informercorev1.NewNamespaceInformer(kif, time.Minute, indexers)
	nsInformer.AddEventHandler(handler)
	return nsInformer, nil
}
