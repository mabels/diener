package k8scrds

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type DienerV1Alpha1Interface interface {
	S3Backends(namespace string) S3BackendInterface
}

type DienerV1Alpha1Client struct {
	restClient rest.Interface
	ctx        context.Context
}

const GroupName = "diener.adviser.com"
const GroupVersion = "v1alpha1"

var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: GroupVersion}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&S3Backend{},
		&S3BackendList{},
	)

	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func NewForConfig(ctx context.Context, c *rest.Config) (DienerV1Alpha1Interface, error) {
	config := *c
	config.ContentConfig.GroupVersion = &schema.GroupVersion{Group: GroupName, Version: GroupVersion}
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}

	return &DienerV1Alpha1Client{restClient: client, ctx: ctx}, nil
}

func (c *DienerV1Alpha1Client) S3Backends(namespace string) S3BackendInterface {
	return &s3BackendClient{
		restClient: c.restClient,
		ns:         namespace,
		ctx:        c.ctx,
	}
}

func NewS3BackendInformer(clientSet DienerV1Alpha1Interface, ns string, handler cache.ResourceEventHandler) (cache.Store, cache.Controller) {
	projectStore, projectController := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(lo metav1.ListOptions) (result runtime.Object, err error) {
				return clientSet.S3Backends(ns).List(lo)
			},
			WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
				return clientSet.S3Backends(ns).Watch(lo)
			},
		},
		&S3Backend{},
		1*time.Minute,
		handler,
	)
	return projectStore, projectController
}
