package example

import (
	"context"
	"io"

	. "github.com/aos-dev/go-storage/v3/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	panic("not implemented")
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	panic("not implemented")
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	panic("not implemented")
}

func (s *Storage) metadata(ctx context.Context, opt pairStorageMetadata) (meta *StorageMeta, err error) {
	panic("not implemented")
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	panic("not implemented")
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	panic("not implemented")
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	panic("not implemented")
}
