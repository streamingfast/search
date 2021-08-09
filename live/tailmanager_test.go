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

package live

import (
	"fmt"
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/streamingfast/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTailManagerTruncateTwice(t *testing.T) {

	tests := []struct {
		name                 string
		initialTailBlock     uint64
		searchPeers          []*dmesh.SearchPeer
		minTargetBufferSize  int
		initialBufferContent []bstream.BlockRef
		lowestTailLock       uint64

		expectedNoTruncationPublish bool
		expectedNoTruncationApply   bool

		expectedSearchPeerTailBlock        uint64
		expectedBufferContent              []bstream.BlockRef
		expectedSearchPeerTailBlock2ndPass uint64
		expectedBufferContent2ndPass       []bstream.BlockRef
	}{
		{
			name:             "truncates a block",
			initialTailBlock: 10,
			searchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: true,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  true,
				},
			},
			minTargetBufferSize: 2,
			initialBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			lowestTailLock: 12,

			expectedSearchPeerTailBlock:        11,
			expectedSearchPeerTailBlock2ndPass: 11,

			expectedBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedBufferContent2ndPass: []bstream.BlockRef{
				testBlk(11), testBlk(12),
			},
		},
		{
			name:             "does not truncate on lock",
			initialTailBlock: 10,
			searchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: true,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  true,
				},
			},
			minTargetBufferSize: 2,
			initialBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			lowestTailLock: 10,

			expectedSearchPeerTailBlock:        11, // truncation announced
			expectedSearchPeerTailBlock2ndPass: 11,

			expectedBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedBufferContent2ndPass: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12), // not actually truncated because it is locked
			},
		},
		{
			name:             "truncates on no lock",
			initialTailBlock: 10,
			searchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: true,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  true,
				},
			},
			minTargetBufferSize: 2,
			initialBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			lowestTailLock: 0,

			expectedSearchPeerTailBlock:        11,
			expectedSearchPeerTailBlock2ndPass: 11,

			expectedBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedBufferContent2ndPass: []bstream.BlockRef{
				testBlk(11), testBlk(12),
			},
		},
		{
			name:             "does not truncate when buffer too small ",
			initialTailBlock: 10,
			searchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: true,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  true,
				},
			},
			initialBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			minTargetBufferSize: 10,
			lowestTailLock:      0,

			expectedBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedBufferContent2ndPass: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},

			expectedSearchPeerTailBlock:        10,
			expectedSearchPeerTailBlock2ndPass: 10,
		},
		{
			name:             "ignores unready or non-moving archives",
			initialTailBlock: 10,
			searchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: false,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  true,
				},
				{
					GenericPeer: dmesh.GenericPeer{
						Ready: true,
					},
					BlockRangeData: dmesh.NewTestBlockRangeData(1, 11, 11),
					HasMovingHead:  false,
				},
			},
			initialBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			minTargetBufferSize: 2,
			lowestTailLock:      0,

			expectedBufferContent: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedBufferContent2ndPass: []bstream.BlockRef{
				testBlk(10), testBlk(11), testBlk(12),
			},
			expectedSearchPeerTailBlock:        10,
			expectedSearchPeerTailBlock2ndPass: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			searchPeer := &dmesh.SearchPeer{
				GenericPeer:    dmesh.GenericPeer{},
				BlockRangeData: dmesh.NewTestBlockRangeData(test.initialTailBlock, 1000, 1000),
			}
			getSearchPeersFunc := func() []*dmesh.SearchPeer {
				return test.searchPeers
			}
			buffer := bstream.NewBuffer("test TailManager", zlog)
			for _, blk := range test.initialBufferContent {
				buffer.AppendHead(blk)
			}
			tl := bstream.NewTailLock()
			if test.lowestTailLock != 0 {
				tl.TailLock(test.lowestTailLock)
			}
			mesh, err := dmeshClient.New("local://")
			require.NoError(t, err)
			tm := &TailManager{
				tailLock:            tl,
				dmeshClient:         mesh,
				searchPeer:          searchPeer,
				getSearchPeers:      getSearchPeersFunc,
				buffer:              buffer,
				minTargetBufferSize: test.minTargetBufferSize,
				backendThreshold:    1,
			}

			tm.attemptTruncation()
			assert.Equal(t, test.expectedSearchPeerTailBlock, searchPeer.TailBlock)
			assert.Equal(t, test.expectedBufferContent, buffer.AllBlocks())

			tm.attemptTruncation()
			assert.Equal(t, test.expectedSearchPeerTailBlock2ndPass, searchPeer.TailBlock)
			assert.Equal(t, test.expectedBufferContent2ndPass, buffer.AllBlocks())
		})
	}
}

func testBlk(num uint64) *bstream.PreprocessedBlock {
	prevNum := uint64(0)
	if num > 1 {
		prevNum = num - 1
	}
	id := fmt.Sprintf("%08xa", num)
	prev := fmt.Sprintf("%08xa", prevNum)

	return &bstream.PreprocessedBlock{
		Block: bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev": %q}`, id, prev)),
		Obj:   &search.SingleIndex{},
	}
}
