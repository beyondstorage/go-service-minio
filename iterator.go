package minio

import "github.com/minio/minio-go/v7"

// list iterator page status struct
type objectPageStatus struct {
	// bufferSize limit for one time buffer item num
	bufferSize int
	// use to generate unique name
	uniquePath string
	// list path
	dir string

	// use to identify starting
	started           bool
	continuationToken string

	// done
	done bool

	// next page
	nextPage bool

	// iterator object channel
	objChan <-chan minio.ObjectInfo
}

func (o *objectPageStatus) ContinuationToken() string {
	return o.continuationToken
}
