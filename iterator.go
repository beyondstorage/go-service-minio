package minio

import "github.com/minio/minio-go/v7"

type storagePageStatus struct{}

func (i *storagePageStatus) ContinuationToken() string {
	return ""
}

type objectPageStatus struct {
	bufferSize int
	counter    int

	objChan <-chan minio.ObjectInfo
}

func (i *objectPageStatus) ContinuationToken() string {
	return string(i.counter)
}
