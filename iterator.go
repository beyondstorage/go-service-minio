package minio

import (
	"github.com/minio/minio-go/v7"
	"strconv"
)

type storagePageStatus struct {
	bufferSize int
	total      int
	remain     int

	buckets []minio.BucketInfo
}

func (i *storagePageStatus) ContinuationToken() string {
	return strconv.Itoa(i.total - i.remain)
}

type objectPageStatus struct {
	bufferSize int
	counter    int

	objChan <-chan minio.ObjectInfo
}

func (i *objectPageStatus) ContinuationToken() string {
	return strconv.Itoa(i.counter)
}
