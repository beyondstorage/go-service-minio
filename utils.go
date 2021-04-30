package example

import (
	"github.com/aos-dev/go-storage/v3/types"
)

// Storage is the example client.
type Storage struct {
	pairPolicy   types.PairPolicy
	defaultPairs DefaultStoragePairs
}

// String implements Storager.String
func (s *Storage) String() string {
	panic("implement me")
}

// NewStorager will create Storager only.
func NewStorager(pairs ...*types.Pair) (types.Storager, error) {
	panic("implement me")
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	panic("implement me")
}
