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
	"math/rand"
	"time"

	"github.com/streamingfast/dmesh"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// dmeshPlanner is the engine that dispatches queries to the different
// backend nodes, based on the state of the available services
// (through the `dmesh` discovery package), and the range of an
// incoming query.

var getSeed func() int64

func init() {
	getSeed = func() int64 {
		return time.Now().Unix()
	}
}

type Planner interface {
	NextPeer(lowBlockNum uint64, highBlockNum uint64, descending bool, withReversible bool) *PeerRange
}

type dmeshPlanner struct {
	peers              func() []*dmesh.SearchPeer
	headDelayTolerance uint64
}

func NewDmeshPlanner(peerFetcher func() []*dmesh.SearchPeer, liveDriftThreshold uint64) *dmeshPlanner {
	return &dmeshPlanner{
		peers:              peerFetcher,
		headDelayTolerance: liveDriftThreshold,
	}
}

func (s *dmeshPlanner) NextPeer(lowBlockNum uint64, highBlockNum uint64, descending bool, withReversible bool) *PeerRange {
	zlog.Debug("finding peers for range",
		zap.Uint64("low_block_num", lowBlockNum),
		zap.Uint64("high_block_num", highBlockNum),
		zap.Bool("descending", descending),
		zap.Bool("with_reversible", withReversible))

	peers := getReadyPeers(s.peers())

	var candidatePeers []*dmesh.SearchPeer

	highestSeenTierLevel := uint32(0)
	for _, peer := range peers {
		if peerCanServeRange(peer, withReversible, descending, highBlockNum, lowBlockNum, s.headDelayTolerance) {
			candidatePeers = append(candidatePeers, peer)
			if peer.TierLevel > highestSeenTierLevel {
				highestSeenTierLevel = peer.TierLevel
			}
		}
	}

	if len(candidatePeers) == 0 {
		return nil
	}

	var highestTierPeers []*dmesh.SearchPeer
	for _, peer := range candidatePeers {
		if peer.TierLevel == highestSeenTierLevel {
			highestTierPeers = append(highestTierPeers, peer)
		}
	}

	idx := rand.New(rand.NewSource(getSeed())).Int() % len(highestTierPeers)
	selectedPeer := highestTierPeers[idx]

	zlog.Debug("dmesh planner peer selected", zap.Reflect("search_peer", selectedPeer), zap.Any("highest_tier_peers", highestTierPeers), zap.Any("all_candidate_peers", candidatePeers))
	return getPeerRange(lowBlockNum, highBlockNum, selectedPeer, descending, withReversible)
}

type PeerRange struct {
	Conn             *grpc.ClientConn
	Addr             string
	LowBlockNum      uint64
	HighBlockNum     uint64
	ServesReversible bool
}

func NewPeerRange(peer *dmesh.SearchPeer, lowBlockNum, highBlockNum uint64) *PeerRange {
	return &PeerRange{
		Conn:             peer.Conn(),
		Addr:             peer.Addr(),
		LowBlockNum:      lowBlockNum,
		HighBlockNum:     highBlockNum,
		ServesReversible: peer.ServesReversible,
	}
}

func getPeerLastBlockNum(peer *dmesh.SearchPeer, withReversible bool) uint64 {
	if peer.ServesReversible && withReversible {
		return peer.HeadBlock
	}
	return peer.IrrBlock
}

func getPeerRange(lowBlockNum uint64, highBlockNum uint64, peer *dmesh.SearchPeer, descending bool, withReversible bool) *PeerRange {
	var low uint64
	var high uint64
	if descending {
		high = highBlockNum

		low = peer.TailBlock
		if low < lowBlockNum {
			low = lowBlockNum
		}

	} else {
		low = lowBlockNum

		if peer.ServesReversible {
			// since the peer serves reversible blocks, the ascending query will continue till it reaches it's desired high block num
			high = highBlockNum
		} else {
			// since peers serves irreversible blocks, the ascending query will continue till the smallest high block num
			virtualHeadBlock := getPeerLastBlockNum(peer, withReversible)
			if virtualHeadBlock < highBlockNum {
				high = virtualHeadBlock
			} else {
				high = highBlockNum
			}
		}
	}

	return NewPeerRange(peer, low, high)
}

func peerCanServeRange(peer *dmesh.SearchPeer, withReversible bool, descending bool, highBlockNum uint64, lowBlockNum uint64, headDelayTolerance uint64) bool {
	peerLowBlockNum := peer.TailBlock
	peerHighBlockNum := getPeerLastBlockNum(peer, withReversible)

	// It is possible that when you query a moving head backend (like a live) that has
	// drifted, the virtual head of said backend, may be below the
	// query's low block num, succesfully serviced by a non-lagging previous peers. We would want to let
	// that query go through when it within a certain block threshold, with the assumption that the
	// backend willl 'catchup' with the request low block num.
	// TODO: when evaluating search archive backend... do we want to do this check? rather then take one backend where you would not wait

	if descending {
		if peer.ServesReversible {
			return (peerHighBlockNum+headDelayTolerance) >= highBlockNum && peerLowBlockNum <= highBlockNum
		}
		return peerHighBlockNum >= highBlockNum && peerLowBlockNum <= highBlockNum
	}

	if peer.ServesReversible {
		return peerLowBlockNum <= lowBlockNum && (peerHighBlockNum+headDelayTolerance) >= lowBlockNum
	}

	return peerLowBlockNum <= lowBlockNum && peerHighBlockNum >= lowBlockNum
}
