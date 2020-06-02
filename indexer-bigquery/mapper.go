package indexer_bigquery

import (
	"github.com/dfuse-io/bstream"
)

type Mapper struct {
	blockMapper BigQueryBlockMapper
}

type BigQueryBlockMapper interface {
	Map(block *bstream.Block) ([]interface{}, error)
}

func NewMapper(blockMapper BigQueryBlockMapper) (*Mapper, error) {
	return &Mapper{blockMapper: blockMapper}, nil
}