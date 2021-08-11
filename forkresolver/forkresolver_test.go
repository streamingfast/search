// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package forkresolver

import (
	"context"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dstore"
	pb "github.com/streamingfast/pbgo/dfuse/search/v1"
	"github.com/streamingfast/dmesh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_FetchMatchingBlocks(t *testing.T) {

	store := dstore.NewMockStore(nil)
	peer := &dmesh.SearchPeer{}

	store.SetFile("0000000100",
		[]byte(`{"id":"aaaaaaaa","num":100}`+"\n"+
			`{"id":"bbbbbbbb","num":101}`+"\n"+
			`{"id":"cccccccc","num":102}`),
	)
	//	store.SetFile("0000000200", []byte(`{"id":"dddddddd","num":200}`))

	fr := NewForkResolver(store, nil, peer, ":9000", ":8080", nil, &mockBlockMapper{IndexMappingImpl: mapping.NewIndexMapping()}, "/tmp")
	blocks, lib, err := fr.getBlocksDescending(context.Background(),
		[]*pb.BlockRef{
			{
				BlockID:  "bbbbbbbb",
				BlockNum: 101,
			},
			{
				BlockID:  "aaaaaaaa",
				BlockNum: 100,
			},
			{
				BlockID:  "cccccccc",
				BlockNum: 102,
			},
		},
	)

	require.NoError(t, err)

	assert.Len(t, blocks, 3)
	assert.Equal(t, uint64(102), blocks[0].Number)
	assert.Equal(t, "cccccccc", blocks[0].ID())
	assert.Equal(t, uint64(101), blocks[1].Number)
	assert.Equal(t, "bbbbbbbb", blocks[1].ID())
	assert.Equal(t, uint64(100), blocks[2].Number)
	assert.Equal(t, "aaaaaaaa", blocks[2].ID())
	assert.Equal(t, uint64(99), lib)

}

type mockBlockMapper struct {
	*mapping.IndexMappingImpl
}

func (b *mockBlockMapper) Map(block *bstream.Block) ([]*document.Document, error) {
	panic("not implemented")
}
