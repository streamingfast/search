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

package router

import (
	"context"
	"testing"

	"github.com/dfuse-io/bstream"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"github.com/streamingfast/dmesh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func Test_GetSearchHighestHeadInfo(t *testing.T) {
	tests := []struct {
		name            string
		peers           []*dmesh.SearchPeer
		expectHeadBlock uint64
		expectIrrBlock  uint64
	}{
		{
			name: "single archive",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-frozen"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 20, 20), TierLevel: 1},
			},
			expectHeadBlock: 20,
			expectIrrBlock:  20,
		},
		{
			name: "two archives",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 34, 42), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(21, 36, 41), TierLevel: 2},
			},
			expectHeadBlock: 42,
			expectIrrBlock:  36,
		},
		{
			name: "three archives",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 20, 20), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(21, 40, 40), TierLevel: 2},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(21, 40, 40), TierLevel: 2},
			},
			expectHeadBlock: 40,
			expectIrrBlock:  40,
		},
		{
			name: "six test",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-0-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 20, 20), TierLevel: 0},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-0-2"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 19, 19), TierLevel: 0},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-1-1"), BlockRangeData: dmesh.NewTestBlockRangeData(19, 39, 39), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-1-2"), BlockRangeData: dmesh.NewTestBlockRangeData(20, 41, 42), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-2-1"), BlockRangeData: dmesh.NewTestBlockRangeData(35, 39, 39), TierLevel: 2},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-tier-2-2"), BlockRangeData: dmesh.NewTestBlockRangeData(31, 40, 44), TierLevel: 2},
			},
			expectHeadBlock: 44,
			expectIrrBlock:  41,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			head, irr := getSearchHighestHeadInfo(test.peers)
			assert.Equal(t, test.expectHeadBlock, head)
			assert.Equal(t, test.expectIrrBlock, irr)
		})
	}
}

func TestSetComplete(t *testing.T) {
	tests := []struct {
		name                 string
		sharder              *queryExecutor
		trailer              metadata.MD
		expectReangeComplete []string
		expectLength         int
	}{
		{
			name: "Range completed",
			sharder: &queryExecutor{
				limitReached:    false,
				incompleteRange: false,
			},
			trailer:              metadata.MD{},
			expectReangeComplete: []string{"true"},
			expectLength:         1,
		},
		{
			name: "Query sharder limit reached",
			sharder: &queryExecutor{
				limitReached:    true,
				incompleteRange: false,
			},
			trailer:      metadata.MD{},
			expectLength: 0,
		},
		{
			name: "Unbounded ascending paginated query should never compelete range",
			sharder: &queryExecutor{
				limitReached:    false,
				incompleteRange: true,
			},
			trailer:      metadata.MD{},
			expectLength: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setComplete(test.trailer, test.sharder)

			require.Len(t, test.trailer.Get("range-completed"), test.expectLength)
			require.Equal(t, test.expectReangeComplete, test.trailer.Get("range-completed"))
		})
	}
}

func Test_CursorStillValid(t *testing.T) {
	tests := []struct {
		name                       string
		libnum                     uint64
		cursor                     *cursor
		blockmetaResp              string
		expectBlockmetaReqBlockNum uint64
		expectBlockmetaCalled      bool
		expectError                string
	}{
		{
			name: "no ID",
			cursor: &cursor{
				blockNum:  100,
				trxPrefix: "abc",
			},
			libnum: 101,
		},
		{
			name: "passed LIB, valid",
			cursor: &cursor{
				blockNum:    8,
				headBlockID: "00000008a",
				trxPrefix:   "abc",
			},
			expectBlockmetaCalled:      true,
			expectBlockmetaReqBlockNum: 8,
			blockmetaResp:              "00000008a",
			libnum:                     101,
		},
		{
			name: "passed LIB, invalid",
			cursor: &cursor{
				blockNum:    8,
				headBlockID: "00000008a",
				trxPrefix:   "abc",
			},
			expectError:                "cursor is not valid anymore: it points to block 00000008a, which was forked out. The correct block ID at this height is 00000008b",
			expectBlockmetaCalled:      true,
			expectBlockmetaReqBlockNum: 8,
			blockmetaResp:              "00000008b",
			libnum:                     101,
		},
		{
			name: "not passed LIB",
			cursor: &cursor{
				blockNum:    108,
				headBlockID: "00000008a",
				trxPrefix:   "abc",
			},
			expectBlockmetaCalled: false,
			libnum:                101,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metacli := &testBlockIDClient{
				nextAnswer: test.blockmetaResp,
			}
			err := checkCursorStillValid(context.Background(), test.cursor, test.libnum, metacli)
			if test.expectError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectError)
			}
			assert.Equal(t, test.expectBlockmetaReqBlockNum, metacli.lastRequestedNum)
			assert.Equal(t, test.expectBlockmetaCalled, metacli.called)

		})
	}
}

func Test_adjustQueryRange(t *testing.T) {
	tests := []struct {
		name                  string
		queryRange            *QueryRange
		protocolFirstBlockNum uint64
		expectQueryRange      *QueryRange
	}{
		{
			name: "EOS with low block num 0",
			queryRange: &QueryRange{
				lowBlockNum:  0,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 2,
			expectQueryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
		},
		{
			name: "EOS with low block num 1",
			queryRange: &QueryRange{
				lowBlockNum:  1,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 2,
			expectQueryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
		},
		{
			name: "EOS with low block num 2",
			queryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 2,
			expectQueryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
		},
		{
			name: "EOS with low block num 3",
			queryRange: &QueryRange{
				lowBlockNum:  3,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 2,
			expectQueryRange: &QueryRange{
				lowBlockNum:  3,
				highBlockNum: 10,
			},
		},
		{
			name: "ETH with low block num 0",
			queryRange: &QueryRange{
				lowBlockNum:  0,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 0,
			expectQueryRange: &QueryRange{
				lowBlockNum:  0,
				highBlockNum: 10,
			},
		},
		{
			name: "ETH with low block num 1",
			queryRange: &QueryRange{
				lowBlockNum:  1,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 0,
			expectQueryRange: &QueryRange{
				lowBlockNum:  1,
				highBlockNum: 10,
			},
		},
		{
			name: "ETH with low block num 2",
			queryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 0,
			expectQueryRange: &QueryRange{
				lowBlockNum:  2,
				highBlockNum: 10,
			},
		},
		{
			name: "ETH with low block num 3",
			queryRange: &QueryRange{
				lowBlockNum:  3,
				highBlockNum: 10,
			},
			protocolFirstBlockNum: 0,
			expectQueryRange: &QueryRange{
				lowBlockNum:  3,
				highBlockNum: 10,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bstream.GetProtocolFirstStreamableBlock = test.protocolFirstBlockNum
			assert.Equal(t, test.expectQueryRange, adjustQueryRange(test.queryRange))
		})
	}
}

type testBlockIDClient struct {
	lastRequestedNum uint64
	nextAnswer       string
	called           bool
}

func (c *testBlockIDClient) NumToID(_ context.Context, in *pbblockmeta.NumToIDRequest, opts ...grpc.CallOption) (*pbblockmeta.BlockIDResponse, error) {
	c.called = true
	c.lastRequestedNum = in.BlockNum
	return &pbblockmeta.BlockIDResponse{
		Id:           c.nextAnswer,
		Irreversible: true,
	}, nil
}

// for interface only
func (c *testBlockIDClient) LIBID(ctx context.Context, in *pbblockmeta.LIBRequest, opts ...grpc.CallOption) (*pbblockmeta.BlockIDResponse, error) {
	panic("not implemented")
}
