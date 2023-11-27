package k8sinformers

// func xxx() {
// 	log.Debug().Str("ns", obj.(*corev1.Namespace).Name).Msg("add namespace")
// 	ingressInformer := informernetv1.NewIngressInformer(kif, obj.(*corev1.Namespace).Name, time.Minute, indexers)
// 	ingressInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
// 		AddFunc: func(obj interface{}) {
// 			handler.AddFunc(obj)
// 		},
// 		UpdateFunc: func(oldObj, newObj interface{}) {
// 			handler.UpdateFunc(oldObj, newObj)
// 		},
// 		DeleteFunc: func(obj interface{}) {
// 			handler.DeleteFunc(obj)
// 		},
// 	})
// 	s3BackendInformer := NewS3BackendInformer(dienerApi, obj.(*corev1.Namespace).Name, handler)
// 	namespacedInformers.addSubInformer(obj.(*corev1.Namespace).Name, subInformer{
// 		ingressInformer:   ingressInformer,
// 		s3BackendInformer: s3BackendInformer,
// 	})
// 	go func() {
// 		log.Info().Str("ns", obj.(*corev1.Namespace).Name).Msg("starting ingress informer")
// 		ingressInformer.Run(wait.NeverStop)
// 	}()
// 	go func() {
// 		log.Info().Str("ns", obj.(*corev1.Namespace).Name).Msg("starting S3Backend informer")
// 		k8scrds.WatchS3Backends(dienerApi, obj.(*corev1.Namespace).Name, handler)
// 	}()
// }
