package minio

import (
	"context"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

const defaultListObjectBufferSize = 100

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)
	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			return
		}
		rp += "/"
		o = s.newObject(true)
		o.Mode = ModeDir
	} else {
		o = s.newObject(false)
		o.Mode = ModeRead
	}
	o.ID = rp
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)
	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return
		}
		rp += "/"
	}
	err = s.client.RemoveObject(ctx, s.bucket, rp, minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	rp := s.getAbsPath(path)
	options := minio.ListObjectsOptions{WithMetadata: true}
	switch {
	case opt.ListMode.IsDir():
		options.Recursive = false
		if !strings.HasSuffix(rp, "/") {
			rp += "/"
		}
	case opt.ListMode.IsPrefix():
		options.Recursive = true
	default:
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}
	options.Prefix = rp

	input := &objectPageStatus{
		bufferSize: defaultListObjectBufferSize,
		options:    options,
	}
	return NewObjectIterator(ctx, s.nextObjectPage, input), nil
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.bucket
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) nextObjectPage(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)
	if input.objChan == nil {
		input.objChan = s.client.ListObjects(ctx, s.bucket, input.options)
	}

	for i := 0; i < input.bufferSize; i++ {
		v, ok := <-input.objChan
		if !ok {
			return IterateDone
		}
		if v.Err == nil {
			o, err := s.formatFileObject(v)
			if err != nil && err != services.ErrObjectNotExist {
				return err
			}
			if err == services.ErrObjectNotExist {
				continue
			}
			page.Data = append(page.Data, o)
			input.counter++
		}
	}
	return nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)
	output, err := s.client.GetObject(ctx, s.bucket, rp, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	defer output.Close()
	var rc io.ReadCloser = output
	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(output, opt.IoCallback)
	}
	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)
	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return
		}
		rp += "/"
	}
	output, err := s.client.StatObject(ctx, s.bucket, rp, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	o, err = s.formatFileObject(output)
	if err != nil {
		return nil, err
	}
	return
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)
	r = io.LimitReader(r, size)
	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}
	options := minio.PutObjectOptions{}
	if opt.HasContentType {
		options.ContentType = opt.ContentType
	}
	if opt.HasContentMd5 {
		options.SendContentMd5 = true
	}
	if opt.HasStorageClass {
		options.StorageClass = opt.StorageClass
	}
	_, err = s.client.PutObject(ctx, s.bucket, rp, r, size, options)
	if err != nil {
		return 0, err
	}
	return size, nil
}
