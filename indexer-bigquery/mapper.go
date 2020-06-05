package indexer_bigquery

import (
	"github.com/dfuse-io/bstream"
	"github.com/linkedin/goavro/v2"
)

type Mapper struct {
	blockMapper BigQueryBlockMapper
}

type BigQueryBlockMapper interface {
	Map(block *bstream.Block) ([]map[string]interface{}, error)
	GetCodec() *goavro.Codec
}

func NewMapper(blockMapper BigQueryBlockMapper) (*Mapper, error) {
	return &Mapper{blockMapper: blockMapper}, nil
}

func (m *Mapper) PreprocessBlock(blk *bstream.Block) (interface{}, error) {
	return m.MapBlock(blk)
}

func (m *Mapper) MapBlock(blk *bstream.Block) ([]map[string]interface{}, error) {
	batch, err := m.blockMapper.Map(blk)
	if err != nil {
		return nil, err
	}

	return batch, nil
}