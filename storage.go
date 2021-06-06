package minio

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

const defaultListBufferSize = 200

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.Mode = ModeRead
	o.ID = s.getAbsPath(path)
	o.Path = path

	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	var fileVersion string
	if opt.HasFileVersion {
		fileVersion = opt.FileVersion
	}

	objectType := "file"
	if opt.HasObjectType {
		objectType = opt.ObjectType
	}

	uniquePath := s.getAbsPath(path)
	switch objectType {
	case "file":
		err = s.client.RemoveObject(ctx, s.bucketName, uniquePath, minio.RemoveObjectOptions{
			GovernanceBypass: true,
			VersionID:        fileVersion,
		})
		if err != nil {
			return err
		}
	case "directory":
		ctxWithCancel, cancel := context.WithCancel(ctx)
		defer func() {
			// ctxWithCancel will return none-nil if closed/cancelled ...
			if ctxWithCancel.Err() == nil {
				cancel()
			}
		}()

		// minio couldn't stat/delete dir object directly
		// ref: https://github.com/minio/minio-go/issues/803
		// using list to found
		findChan := s.client.ListObjects(ctxWithCancel, s.bucketName, minio.ListObjectsOptions{
			Prefix: uniquePath,
			// recursively remove object
			Recursive: true,
		})

		errChan := s.client.RemoveObjects(ctxWithCancel, s.bucketName, findChan, minio.RemoveObjectsOptions{})
		if errChan != nil {
			// if none error happens, minio-client will close this channel which have one buffer
			// so an nil error will be returned
			err = (<-errChan).Err
			if err != nil {
				// if error happens, cancel remove through context
				cancel()
			}
			return err
		}
	default:
		return services.PairUnsupportedError{Pair: Pair{Key: "object_type", Value: opt.ObjectType}}
	}

	return
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	// TODO: more list_mode

	limit := defaultListBufferSize
	if opt.HasListBufferSize {
		limit = opt.ListBufferSize
	}
	iteratorObj := objectPageStatus{
		bufferSize:        limit,
		uniquePath:        s.getAbsPath(path),
		dir:               filepath.ToSlash(path),
		started:           !opt.HasContinuationToken,
		continuationToken: opt.ContinuationToken,
	}

	// list must be a directory
	if !strings.HasSuffix(iteratorObj.uniquePath, "/") {
		iteratorObj.uniquePath += "/"
	}
	objectCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    iteratorObj.uniquePath,
		Recursive: false,
	})

	iteratorObj.objChan = objectCh

	return NewObjectIterator(ctx, s.listNextObject, &iteratorObj), nil
}

func (s *Storage) listNextObject(ctx context.Context, page *ObjectPage) error {
	iteratorObj := page.Status.(*objectPageStatus)

	if iteratorObj.done {
		return IterateDone
	}

	if iteratorObj.nextPage {
		// next page, clear buffer
		page.Data = []*Object{}
	}

	bufferNum := 0
	if !iteratorObj.started {
		for object := range iteratorObj.objChan {
			if object.Err != nil {
				return object.Err
			}
			if s.compareObjectKey(object.Key, iteratorObj.continuationToken) {
				page.Data = append(page.Data, s.formatObject(&object, iteratorObj.uniquePath, iteratorObj.dir))
				bufferNum++
				iteratorObj.started = true
				break
			}
		}
	}

	for object := range iteratorObj.objChan {
		if object.Err != nil {
			return object.Err
		}
		page.Data = append(page.Data, s.formatObject(&object, iteratorObj.uniquePath, iteratorObj.dir))
		bufferNum++
		if bufferNum >= iteratorObj.bufferSize {
			break
		}
	}

	if bufferNum < iteratorObj.bufferSize {
		iteratorObj.done = true
		iteratorObj.nextPage = false
		return IterateDone
	} else {
		iteratorObj.nextPage = true
	}

	return nil
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	meta.Name = ""

	return
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	var rc io.ReadCloser

	objectPath := s.getAbsPath(path)

	// TODO: support more options
	var fileVersion string
	if opt.HasFileVersion {
		fileVersion = opt.FileVersion
	}
	readObject, err := s.client.GetObject(ctx, s.bucketName, objectPath, minio.GetObjectOptions{VersionID: fileVersion})
	if err != nil {
		return n, err
	}
	defer readObject.Close()

	if opt.HasOffset {
		_, err = readObject.Seek(opt.Offset, 0)
		if err != nil {
			return n, err
		}
	}

	rc = readObject

	if opt.HasSize {
		rc = iowrap.LimitReadCloser(rc, opt.Size)
	}

	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, readObject)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	defer func() {
		if err != nil {
			err = s.formatError("stat", err, path)
		}
	}()

	var fileVersion string
	if opt.HasFileVersion {
		fileVersion = opt.FileVersion
	}

	objectType := "file"
	if opt.HasObjectType {
		objectType = opt.ObjectType
	}

	uniquePath := s.getAbsPath(path)
	dir := filepath.ToSlash(path)
	switch objectType {
	case "file":
		objInfo, err := s.client.StatObject(ctx, s.bucketName, uniquePath, minio.StatObjectOptions{VersionID: fileVersion})
		if err != nil {
			return o, err
		}
		o = s.newObject(true)
		o.ID = uniquePath
		o.Path = path
		o.Mode |= ModeRead
		o.SetEtag(objInfo.ETag)
		o.SetLastModified(objInfo.LastModified)
		o.SetContentLength(objInfo.Size)
	case "directory":
		// minio couldn't stat/delete dir object directly
		// ref: https://github.com/minio/minio-go/issues/803
		// using list to found

		// list will return key like prefix/dir/
		// with '/' suffix and without '/' prefix
		// so handle refer object name first

		objKey := uniquePath
		if strings.HasPrefix(objKey, "/") {
			objKey = objKey[1:]
		}
		if !strings.HasSuffix(objKey, "/") {
			objKey = objKey + "/"
		}

		ctxWithCancel, cancel := context.WithCancel(ctx)
		defer func() {
			// ctxWithCancel will return none-nil if closed/cancelled ...
			// return nil if still opening
			if ctxWithCancel.Err() == nil {
				cancel()
			}
		}()

		findChan := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
			Prefix:    uniquePath,
			Recursive: false,
		})

		for object := range findChan {
			if object.Err != nil {
				cancel()
				return
			}

			// return key like prefix/dir/ whit '/' suffix and without '/' prefix
			if object.Key == objKey {
				// already found, cancel
				cancel()
				o = s.formatFolderObject(&object, uniquePath, dir)
				break
			}
		}
	default:
		return o, services.PairUnsupportedError{Pair: Pair{Key: "object_type", Value: opt.ObjectType}}
	}

	return
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)

	uploadInfo, err := s.client.PutObject(ctx, s.bucketName, rp, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return n, err
	}

	return uploadInfo.Size, nil
}
