package minio

import (
	"context"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	. "github.com/beyondstorage/go-storage/v4/types"
	"github.com/minio/minio-go/v7"
)

const defaultListStoragerBufferSize = 50

func (s *Service) create(ctx context.Context, name string, opt pairServiceCreate) (store Storager, err error) {
	st, err := s.newStorage(ps.WithName(name))
	if err != nil {
		return nil, err
	}
	err = s.service.MakeBucket(ctx, name, minio.MakeBucketOptions{})
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Service) delete(ctx context.Context, name string, opt pairServiceDelete) (err error) {
	err = s.service.RemoveBucket(ctx, name)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) get(ctx context.Context, name string, opt pairServiceGet) (store Storager, err error) {
	st, err := s.newStorage(ps.WithName(name))
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Service) list(ctx context.Context, opt pairServiceList) (sti *StoragerIterator, err error) {
	input := &storagePageStatus{
		bufferSize: defaultListStoragerBufferSize,
	}
	input.buckets, err = s.service.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	input.total = len(input.buckets)
	input.remain = input.total
	return NewStoragerIterator(ctx, s.nextStoragePage, input), nil
}

func (s *Service) nextStoragePage(ctx context.Context, page *StoragerPage) error {
	input := page.Status.(*storagePageStatus)
	if input.remain < input.bufferSize {
		input.bufferSize = input.remain
	}
	for i := 0; i < input.bufferSize; i++ {
		store, err := s.newStorage(ps.WithName(input.buckets[i].Name))
		if err != nil {
			return err
		}
		page.Data = append(page.Data, store)
	}
	input.buckets = input.buckets[input.bufferSize:]
	input.remain -= input.bufferSize
	if input.remain == 0 {
		return IterateDone
	}
	return nil
}
