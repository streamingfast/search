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

	"github.com/streamingfast/derr"
	"github.com/streamingfast/dmesh"
	"github.com/streamingfast/search/metrics"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

func (r *Router) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	return &pbhealth.HealthCheckResponse{
		Status: r.healthStatus(),
	}, nil
}

func (r *Router) Watch(req *pbhealth.HealthCheckRequest, stream pbhealth.Health_WatchServer) error {
	currentStatus := pbhealth.HealthCheckResponse_SERVICE_UNKNOWN
	waitTime := 0 * time.Second

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(waitTime):
			newStatus := r.healthStatus()
			waitTime = 5 * time.Second

			if newStatus != currentStatus {
				currentStatus = newStatus

				if err := stream.Send(&pbhealth.HealthCheckResponse{Status: currentStatus}); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Router) healthStatus() pbhealth.HealthCheckResponse_ServingStatus {
	status := pbhealth.HealthCheckResponse_NOT_SERVING

	if r.ready.Load() && !derr.IsShuttingDown() {
		status = pbhealth.HealthCheckResponse_SERVING
	}

	return status
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
		absoluteTruncation := uint64(adjustNegativeValues(r.truncationLowBlockNum, int64(headBlockNum)))

		if hasContiguousBlockRange(absoluteTruncation, headBlockNum, readyPeers) {
			metrics.FullContiguousBlockRange.SetUint64(1)
		} else {
			metrics.FullContiguousBlockRange.SetUint64(0)
		}
		time.Sleep(8 * time.Second)
	}
}

func hasContiguousBlockRange(absoluteTruncationLowBlockNum, blockNum uint64, peers []*dmesh.SearchPeer) bool {
	lowestBlockNum := uint64(math.MaxUint64)

	for _, peer := range peers {
		if (peer.HeadBlock >= blockNum) && (peer.TailBlock < blockNum) {
			if peer.TailBlock < lowestBlockNum {
				lowestBlockNum = peer.TailBlock
			}
		}
	}

	if lowestBlockNum <= absoluteTruncationLowBlockNum {
		return true
	} else if lowestBlockNum == uint64(math.MaxUint64) {
		return false
	} else {
		return hasContiguousBlockRange(absoluteTruncationLowBlockNum, (lowestBlockNum - 1), peers)
	}
}

func sortPeers(peers []*dmesh.SearchPeer) {
	sort.Slice(peers, func(i, j int) bool {
		// Old blocks are lower tier, more changes to increase the
		// number of components at higher tier levels
		return peers[i].TierLevel > peers[j].TierLevel
	})
}
