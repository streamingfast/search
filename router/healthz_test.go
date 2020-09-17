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
	"testing"

	"github.com/dfuse-io/dmesh"
	"github.com/stretchr/testify/assert"
)

func TestCheckContiguousBlockRange(t *testing.T) {
	tests := []struct {
		name                          string
		headBlockNum                  uint64
		peers                         []*dmesh.SearchPeer
		expectState                   bool
		absoluteTruncationLowBlockNum uint64
	}{
		{
			name:         "contiguous range with one backend",
			headBlockNum: 93422092,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 93421780, 93422092), TierLevel: 1},
			},
			expectState: true,
		},
		{
			name:         "contiguous range with two backend",
			headBlockNum: 93422077,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(87000000, 93421760, 93422077), TierLevel: 20},
			},
			expectState: true,
		},
		{
			name:         "contiguous range with multiple backend",
			headBlockNum: 93422092,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(87000000, 93421760, 93422077), TierLevel: 20},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(93313200, 93421763, 93422077), TierLevel: 40},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-4"), BlockRangeData: dmesh.NewTestBlockRangeData(93421763, 93421780, 93422092), TierLevel: 100},
			},
			expectState: true,
		},
		{
			name:         "none contiguous range with two backend",
			headBlockNum: 93422077,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(87000001, 93421760, 93422077), TierLevel: 20},
			},
			expectState: false,
		},
		{
			name:         "none contiguous range with multiple backend",
			headBlockNum: 93422077,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(87000000, 90000000, 90000000), TierLevel: 20},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(92000000, 93421763, 93422077), TierLevel: 40},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-4"), BlockRangeData: dmesh.NewTestBlockRangeData(93421763, 93421780, 93422092), TierLevel: 100},
			},
			expectState: false,
		},
		{
			name:         "contiguous range does not go to 0",
			headBlockNum: 93422092,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(86999999, 90000000, 90000000), TierLevel: 20},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(92000000, 93421763, 93422077), TierLevel: 40},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-4"), BlockRangeData: dmesh.NewTestBlockRangeData(93421763, 93421780, 93422092), TierLevel: 100},
			},
			expectState: false,
		},
		{
			name:         "contiguous range does not go to 0 but goes below truncation",
			headBlockNum: 93422092,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(86999999, 90000000, 90000000), TierLevel: 20}, // hole
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(92000000, 93421763, 93422077), TierLevel: 40},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-4"), BlockRangeData: dmesh.NewTestBlockRangeData(93421763, 93421780, 93422092), TierLevel: 100},
			},
			absoluteTruncationLowBlockNum: 92000000,
			expectState:                   true,
		},
		{
			name:         "contiguous range does not go to 0 or even truncation",
			headBlockNum: 93422092,
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 86999999, 86999999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(86999999, 90000000, 90000000), TierLevel: 20}, // hole
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-3"), BlockRangeData: dmesh.NewTestBlockRangeData(92000000, 93421763, 93422077), TierLevel: 40},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-4"), BlockRangeData: dmesh.NewTestBlockRangeData(93421763, 93421780, 93422092), TierLevel: 100},
			},
			absoluteTruncationLowBlockNum: 100,
			expectState:                   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectState, hasContiguousBlockRange(test.absoluteTruncationLowBlockNum, test.headBlockNum, test.peers))
		})
	}
}
