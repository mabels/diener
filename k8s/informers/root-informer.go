package k8sinformers

import (
	"time"

	"github.com/rs/zerolog"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	corev1 "k8s.io/api/core/v1"

	informercorev1 "k8s.io/client-go/informers/core/v1"
	informernetv1 "k8s.io/client-go/informers/networking/v1"

	kubernetes "k8s.io/client-go/kubernetes"
)

func RootInformer(config *rest.Config, log zerolog.Logger, handler cache.ResourceEventHandlerFuncs) (cache.SharedIndexInformer, error) {
	kif, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error().Err(err).Msg("new for config")
		return nil, err
	}

	indexers := map[string]cache.IndexFunc{}
	nsInformer := informercorev1.NewNamespaceInformer(kif, time.Minute, indexers)
	namespacedInformers := map[string]cache.SharedIndexInformer{}
	nsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			handler.AddFunc(obj)
			log.Debug().Str("ns", obj.(*corev1.Namespace).Name).Msg("add namespace")
			ingressInformer := informernetv1.NewIngressInformer(kif, obj.(*corev1.Namespace).Name, time.Minute, indexers)
			ingressInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					handler.AddFunc(obj)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					handler.UpdateFunc(oldObj, newObj)
				},
				DeleteFunc: func(obj interface{}) {
					handler.DeleteFunc(obj)
				},
			})
			namespacedInformers[obj.(*corev1.Namespace).Name] = ingressInformer
			go func() {
				ingressInformer.Run(wait.NeverStop)
			}()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			handler.UpdateFunc(oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			handler.DeleteFunc(obj)
		},
	})
	return nsInformer, nil
}
