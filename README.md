# diener
A kubernets ingress controller with multiple backends like S3

The CRD for the S3Backend

https://github.com/mabels/diener/k8s-crds/s3backends.diener.adviser.com.crd.yaml

we need to add the ingressClass
```
apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata:
  name: diener
spec:
  controller: diener.adviser.com/controller
```

sample:
```
apiVersion: diener.adviser.com/v1alpha1
kind: S3Backend
metadata:
  name: example
spec:
    accessKey: "accessKey"
    bucketName: "bucketName"
    endpoint: "http://doof"
    maxAgeSeconds: 3700
    maxObjectSize: 10000000
    region: "us-west-1"
    secretKey: "geheim"
    transferBufSize: 2939393
```

the matching ingress looks like:
```
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: pictures.whatever.tech
spec:
  ingressClassName: diener
  rules:
    - http:
        paths:
          - path: /
            pathType: ImplementationSpecific
            backend:
              resource:
                apiGroup: diener.adviser.com
                kind: S3Backend
                name: picture
```
