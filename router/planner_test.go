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

	"github.com/streamingfast/dmesh"
	"github.com/stretchr/testify/assert"
)

func TestDmeshPlanner_NextPeer(t *testing.T) {
	tests := []struct {
		name               string
		peers              []*dmesh.SearchPeer
		liveDriftThreshold uint64
		lowBlockNum        uint64
		highBlockNum       uint64
		descending         bool
		withReversible     bool
		expectPeerRange    *PeerRange
	}{
		{
			name: "single archive, with complete ascending request",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 200), TierLevel: 1},
			},
			lowBlockNum:  20,
			highBlockNum: 100,
			expectPeerRange: &PeerRange{
				Addr:             "archive-segment-1",
				LowBlockNum:      20,
				HighBlockNum:     100,
				ServesReversible: false,
			},
		},
		{
			name: "single archive, with incomplete ascending request",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 200), TierLevel: 1},
			},
			lowBlockNum:  20,
			highBlockNum: 250,
			expectPeerRange: &PeerRange{
				Addr:             "archive-segment-1",
				LowBlockNum:      20,
				HighBlockNum:     200,
				ServesReversible: false,
			},
		},
		{
			name: "no match ascending",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 200), TierLevel: 1},
			},
			lowBlockNum:     201,
			highBlockNum:    250,
			expectPeerRange: nil,
		},
		{
			name: "single archive, with complete descending request",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 300, 300), TierLevel: 1},
			},
			lowBlockNum:  150,
			highBlockNum: 250,
			descending:   true,
			expectPeerRange: &PeerRange{
				Addr:             "archive-segment-1",
				LowBlockNum:      150,
				HighBlockNum:     250,
				ServesReversible: false,
			},
		},
		{
			name: "single archive, with incomplete descening request",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 300, 300), TierLevel: 1},
			},
			lowBlockNum:  50,
			highBlockNum: 250,
			descending:   true,
			expectPeerRange: &PeerRange{
				Addr:             "archive-segment-1",
				LowBlockNum:      100,
				HighBlockNum:     250,
				ServesReversible: false,
			},
		},
		{
			name: "no match descending",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(100, 300, 300), TierLevel: 1},
			},
			lowBlockNum:     50,
			highBlockNum:    75,
			expectPeerRange: nil,
		},
		{
			name: "single live, with incomplete descending request with reversible block",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(135, 197, 220), TierLevel: 1, ServesReversible: true, HasMovingHead: true},
			},
			liveDriftThreshold: 7,
			lowBlockNum:        202,
			highBlockNum:       250,
			descending:         false,
			withReversible:     false,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      202,
				HighBlockNum:     250,
				ServesReversible: true,
			},
		},
		{
			name: "single live, with incomplete descending request with reversible block",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(135, 197, 220), TierLevel: 1, ServesReversible: true, HasMovingHead: true},
			},
			liveDriftThreshold: 3,
			lowBlockNum:        202,
			highBlockNum:       250,
			descending:         true,
			withReversible:     false,
			expectPeerRange:    nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewDmeshPlanner(func() []*dmesh.SearchPeer {
				return test.peers
			}, test.liveDriftThreshold)
			peerRange := p.NextPeer(test.lowBlockNum, test.highBlockNum, test.descending, test.withReversible)
			if peerRange != nil {
				peerRange.Conn = nil
			}
			assert.Equal(t, test.expectPeerRange, peerRange)
		})
	}
}

func TestDmeshPlanner_NextPeer_multiple(t *testing.T) {
	tests := []struct {
		name               string
		peers              []*dmesh.SearchPeer
		liveDriftThreshold uint64
		lowBlockNum        uint64
		highBlockNum       uint64
		descending         bool
		withReversible     bool
		expectPeerRange    *PeerRange
	}{
		{
			name: "two archive, take higher tier",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 250), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-2"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 2}},
			lowBlockNum:  195,
			highBlockNum: 245,
			expectPeerRange: &PeerRange{
				Addr:         "archive-segment-2",
				LowBlockNum:  195,
				HighBlockNum: 200,
			},
		},
		{
			name: "one archive and one live, with ascending request initial call",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 250), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    60,
			highBlockNum:   245,
			withReversible: true,
			expectPeerRange: &PeerRange{
				Addr:         "archive-segment-1",
				LowBlockNum:  60,
				HighBlockNum: 200,
			},
		},
		{
			name: "one archive and one live, with ascending request second call",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 250), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    201,
			highBlockNum:   245,
			withReversible: true,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      201,
				HighBlockNum:     245,
				ServesReversible: true,
			},
		},
		{
			name: "one archive and one live, with descending request initial call",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 250), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    60,
			highBlockNum:   245,
			withReversible: true,
			descending:     true,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      195,
				HighBlockNum:     245,
				ServesReversible: true,
			},
		},
		{
			name: "one archive and one live, with ascending request second call",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "archive-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(1, 200, 250), TierLevel: 1},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    60,
			highBlockNum:   194,
			withReversible: true,
			descending:     true,
			expectPeerRange: &PeerRange{
				Addr:         "archive-segment-1",
				LowBlockNum:  60,
				HighBlockNum: 194,
			},
		},
		{
			name: "unbounded ascending request realtive to live",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-5k-0-87m-1"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 88109999, 88109999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-500-87m-up-0"), BlockRangeData: dmesh.NewTestBlockRangeData(87000000, 92477334, 92477660), TierLevel: 20},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-5k-0-87m-0"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 88039999, 88039999), TierLevel: 10},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-500-87m-up-1"), BlockRangeData: dmesh.NewTestBlockRangeData(87000000, 92477334, 92477662), TierLevel: 20},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-live-5c6cd6c499-wpd5s"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 92477358, 92477687), TierLevel: 100, ServesReversible: true},
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "search-v2-d1-live-5c6cd6c499-bkcdw"), BlockRangeData: dmesh.NewTestBlockRangeData(0, 92477358, 92477680), TierLevel: 100, ServesReversible: true},
			},
			lowBlockNum:    92477687,
			highBlockNum:   9000000000000000000,
			withReversible: true,
			descending:     false,
			expectPeerRange: &PeerRange{
				Addr:             "search-v2-d1-live-5c6cd6c499-wpd5s",
				LowBlockNum:      92477687,
				HighBlockNum:     9000000000000000000,
				ServesReversible: true,
			},
		},
		{
			name: "live segment serving an ascending query with irreversible blocks where low block num and high block num are greater then IRR block",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true, HasMovingHead: true},
			},
			liveDriftThreshold: 20,
			lowBlockNum:        210,
			highBlockNum:       1000,
			withReversible:     false,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      210,
				HighBlockNum:     1000,
				ServesReversible: true,
			},
		},
		{
			name: "live segment serving an ascending query with reversible blocks where low block num is lower then head and high block num is greater then head",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    210,
			highBlockNum:   1000,
			withReversible: true,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      210,
				HighBlockNum:     1000,
				ServesReversible: true,
			},
		},
		{
			name: "live segment serving an ascending query with reversible blocks where low block num and high block num is greater then head",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},
			lowBlockNum:    210,
			highBlockNum:   1000,
			withReversible: true,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      210,
				HighBlockNum:     1000,
				ServesReversible: true,
			},
		},
		{
			name: "ascending query with live segment serving a low block number higher the virtual head, should allow it to pass it it is below threshold",
			peers: []*dmesh.SearchPeer{
				{GenericPeer: dmesh.NewTestReadyGenericPeer("v1", "search", "live-segment-1"), BlockRangeData: dmesh.NewTestBlockRangeData(195, 200, 250), TierLevel: 1, ServesReversible: true},
			},

			lowBlockNum:    210,
			highBlockNum:   1000,
			withReversible: true,
			expectPeerRange: &PeerRange{
				Addr:             "live-segment-1",
				LowBlockNum:      210,
				HighBlockNum:     1000,
				ServesReversible: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewDmeshPlanner(func() []*dmesh.SearchPeer {
				return test.peers
			}, test.liveDriftThreshold)
			peerRange := p.NextPeer(test.lowBlockNum, test.highBlockNum, test.descending, test.withReversible)
			if peerRange != nil {
				peerRange.Conn = nil
			}
			assert.Equal(t, test.expectPeerRange, peerRange)
		})
	}
}
