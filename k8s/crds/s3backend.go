package k8scrds

// https://www.martin-helmich.de/en/blog/kubernetes-crd-client.html
import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type S3BackendSpec struct {
	AccessKey       string  `json:"accessKey"`
	BucketName      string  `json:"bucketName"`
	Endpoint        *string `json:"endpoint,omitempty"`
	MaxAgeSeconds   int     `json:"maxAgeSeconds"`
	MaxObjectSize   int     `json:"maxObjectSize"`
	Region          *string `json:"region,omitempty"`
	SecretKey       string  `json:"secretKey"`
	TransferBufSize int     `json:"transferBufSize"`
}

type S3Backend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec S3BackendSpec `json:"spec"`
}

type S3BackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []S3Backend `json:"items"`
}

const S3BackendResource = "s3backends"

// DeepCopyInto copies all properties of this object into another object of the
// same type that is provided as a pointer.
func (in *S3Backend) DeepCopyInto(out *S3Backend) {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = S3BackendSpec{
		AccessKey:       in.Spec.AccessKey,
		BucketName:      in.Spec.BucketName,
		Endpoint:        in.Spec.Endpoint,
		MaxAgeSeconds:   in.Spec.MaxAgeSeconds,
		MaxObjectSize:   in.Spec.MaxObjectSize,
		Region:          in.Spec.Region,
		SecretKey:       in.Spec.SecretKey,
		TransferBufSize: in.Spec.TransferBufSize,
	}
}

// DeepCopyObject returns a generically typed copy of an object
func (in *S3Backend) DeepCopyObject() runtime.Object {
	out := S3Backend{}
	in.DeepCopyInto(&out)
	return &out
}

// DeepCopyObject returns a generically typed copy of an object
func (in *S3BackendList) DeepCopyObject() runtime.Object {
	out := S3BackendList{}
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta

	if in.Items != nil {
		out.Items = make([]S3Backend, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}

	return &out
}

type S3BackendInterface interface {
	List(opts metav1.ListOptions) (*S3BackendList, error)
	Get(name string, options metav1.GetOptions) (*S3Backend, error)
	Create(*S3Backend) (*S3Backend, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	// ...
}

type s3BackendClient struct {
	restClient rest.Interface
	ns         string
	ctx        context.Context
}

func (c *s3BackendClient) List(opts metav1.ListOptions) (*S3BackendList, error) {
	result := S3BackendList{}
	err := c.restClient.
		Get().
		Namespace(c.ns).
		Resource(S3BackendResource).
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(c.ctx).
		Into(&result)

	return &result, err
}

func (c *s3BackendClient) Get(name string, opts metav1.GetOptions) (*S3Backend, error) {
	result := S3Backend{}
	err := c.restClient.
		Get().
		Namespace(c.ns).
		Resource(S3BackendResource).
		Name(name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(c.ctx).
		Into(&result)

	return &result, err
}

func (c *s3BackendClient) Create(project *S3Backend) (*S3Backend, error) {
	result := S3Backend{}
	err := c.restClient.
		Post().
		Namespace(c.ns).
		Resource(S3BackendResource).
		Body(project).
		Do(c.ctx).
		Into(&result)

	return &result, err
}

func (c *s3BackendClient) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.restClient.
		Get().
		Namespace(c.ns).
		Resource(S3BackendResource).
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(c.ctx)
}
