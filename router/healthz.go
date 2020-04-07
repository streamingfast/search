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
	"math"
	"sort"
	"time"

	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/dmesh"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search/metrics"
)

// Check only validates "router.ready" bool and the shutting down process. Does *not* depend on contiguousness
func (r *Router) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	status := pbhealth.HealthCheckResponse_NOT_SERVING

	if r.ready.Load() && !derr.IsShuttingDown() {
		status = pbhealth.HealthCheckResponse_SERVING
	}

	return &pbhealth.HealthCheckResponse{
		Status: status,
	}, nil

}

func (r *Router) setRouterAvailability() {
	for {
		peers := r.dmeshClient.Peers()
		readyPeers := getReadyPeers(peers)

		if len(readyPeers) > 0 {
			r.ready.Store(true)
		} else {
			r.ready.Store(false)
		}

		headBlockNum, _ := getSearchHighestHeadInfo(readyPeers)
		if hasContiguousBlockRange(headBlockNum, readyPeers) {
			metrics.FullContiguousBlockRange.SetUint64(1)
		} else {
			metrics.FullContiguousBlockRange.SetUint64(0)
		}
		time.Sleep(8 * time.Second)
	}
}

func hasContiguousBlockRange(blockNum uint64, peers []*dmesh.SearchPeer) bool {
	lowestBlockNum := uint64(math.MaxUint64)

	for _, peer := range peers {
		if (peer.HeadBlock >= blockNum) && (peer.TailBlock < blockNum) {
			if peer.TailBlock < lowestBlockNum {
				lowestBlockNum = peer.TailBlock
			}
		}
	}

	if lowestBlockNum == 0 {
		return true
	} else if lowestBlockNum == uint64(math.MaxUint64) {
		return false
	} else {
		return hasContiguousBlockRange((lowestBlockNum - 1), peers)
	}
}

func sortPeers(peers []*dmesh.SearchPeer) {
	sort.Slice(peers, func(i, j int) bool {
		// Old blocks are lower tier, more changes to increase the
		// number of components at higher tier levels
		return peers[i].TierLevel > peers[j].TierLevel
	})
}
