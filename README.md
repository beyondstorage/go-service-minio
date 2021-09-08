[![Build Status](https://github.com/beyondstorage/go-service-minio/workflows/Unit%20Test/badge.svg?branch=master)](https://github.com/beyondstorage/go-service-minio/actions?query=workflow%3A%22Unit+Test%22)
[![License](https://img.shields.io/badge/license-apache%20v2-blue.svg)](https://github.com/Xuanwo/storage/blob/master/LICENSE)
[![](https://img.shields.io/matrix/beyondstorage@go-storage:matrix.org.svg?logo=matrix)](https://matrix.to/#/#beyondstorage@go-storage:matrix.org)

# go-service-minio

[MinIO](https://min.io/) is an open source cloud-native high-performance object storage service. 
This project will use minio's native SDK to implement [go-storage](https://github.com/beyondstorage/go-storage/), 
enabling users to manipulate data on minio servers through a unified interface.

## Install

```go
go get github.com/beyondstorage/go-service-minio
```

## Usage

```go
import (
	"log"

	_ "github.com/beyondstorage/go-service-minio"
	"github.com/beyondstorage/go-storage/v4/services"
)

func main() {
	store, err := services.NewStoragerFromString("minio://<bucket_name>/<work_dir>?credential=hmac:<access_key>:<secret_key>&endpoint=https:<host>:<port>")
	if err != nil {
		log.Fatal(err)
	}
	
	// Write data from io.Reader into hello.txt
	n, err := store.Write("hello.txt", r, length)
}
```

- See more examples in [go-storage-example](https://github.com/beyondstorage/go-storage-example).
- Read [more docs](https://beyondstorage.io/docs/go-storage/services/minio) about go-service-minio.
