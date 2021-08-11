package search

import (
	"context"
	"sort"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmesh"
	"go.uber.org/zap"
)

var DmeshArchiveLIBTarget = bstream.Target("dmesh-head-target")

type GetSearchPeersFunc func() []*dmesh.SearchPeer

func DmeshHighestArchiveBlockRefGetter(getAllPeers GetSearchPeersFunc, backendThreshold int) bstream.BlockRefGetter {
	if backendThreshold == 0 {
		panic("invalid value for backendThresold in DmeshHighestArchiveBlockRefGetter")
	}

	return func(_ context.Context) (bstream.BlockRef, error) {
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
			return irrPeers[len(irrPeers)-backendThreshold], nil
		}
		return nil, bstream.ErrTrackerBlockNotFound
	}
}
