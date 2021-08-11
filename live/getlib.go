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
	"sort"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmesh"
	"go.uber.org/zap"
)

type GetSearchPeersFunc func() []*dmesh.SearchPeer

func GetMeshLIB(getAllPeers GetSearchPeersFunc, backendThreshold int) bstream.BlockRef {
	peers := getAllPeers()

	var irrPeers []bstream.BlockRef
	for _, peer := range peers {
		if !peer.HasMovingHead {
			continue
		}
		if peer.ServesReversible {
			continue
		}
		if !peer.Ready {
			continue
		}

		irrPeers = append(irrPeers, bstream.NewBlockRef(peer.IrrBlockID, peer.IrrBlock))
	}

	sort.Slice(irrPeers, func(i, j int) bool { return irrPeers[i].Num() < irrPeers[j].Num() })

	zlog.Debug("number of peers visible to truncation tracker", zap.Int("count", len(irrPeers)))

	if len(irrPeers) >= backendThreshold {
		return irrPeers[len(irrPeers)-backendThreshold]
	}

	return nil
}
