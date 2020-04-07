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
	"context"
	"fmt"

	v1 "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dmesh"
	"github.com/dfuse-io/search"
	"go.uber.org/zap"
)

type liveQuery struct {
	zlog       *zap.Logger
	searchPeer *dmesh.SearchPeer

	ctx context.Context

	sourceFromBlockNumFunc func(startBlockNum uint64, handler bstream.Handler) (*hub.HubSource, error)
	matchCollector         search.MatchCollector
	protocol               v1.Protocol

	request    *pb.BackendRequest
	bleveQuery *search.BleveQuery

	incomingMatches chan *pb.SearchMatch
	aggregatorDone  chan struct{}
	aggregatorError error

	lastBlockRead uint64

	// fwd only
	liveMarkerReached          bool
	liveMarkerLastSentBlockNum uint64
	// backward only
	backwardBlocks []*indexedBlock
}

func (b *LiveBackend) newLiveQuery(ctx context.Context, request *pb.BackendRequest, bquery *search.BleveQuery) *liveQuery {
	q := &liveQuery{
		sourceFromBlockNumFunc: b.hub.NewHubSourceFromBlockNum,
		matchCollector:         b.matchCollector,
		protocol:               b.protocol,
		searchPeer:             b.searchPeer,
		ctx:                    ctx,
		zlog:                   zlog,
		request:                request,
		incomingMatches:        make(chan *pb.SearchMatch, 100),
		aggregatorDone:         make(chan struct{}),
		bleveQuery:             bquery,
	}

	return q
}

func (q *liveQuery) checkBoundaries(first, irr, head, headDelayTolerance uint64) error {
	virtHead := toVirtualHead(q.request.WithReversible, irr, head)

	if q.request.Descending {
		if q.request.HighBlockNum > virtHead+headDelayTolerance {
			return fmt.Errorf("descending high block num requested (%d) higher than available virtual head (%d) with head delay tolerance (%d)", q.request.HighBlockNum, virtHead, headDelayTolerance)
		}

	} else {
		if q.request.LowBlockNum < first {
			return fmt.Errorf("ascending requested lower boundary (%d) lower than available tail block (%d) on this node", q.request.LowBlockNum, first)
		}

		if (virtHead + headDelayTolerance) <= q.request.LowBlockNum {
			return fmt.Errorf("ascending requested lower boundary (%d) higher than virtual head (%d) with head delay tolerance (%d)", q.request.LowBlockNum, virtHead, headDelayTolerance)
		}

	}

	return nil
}

func (q *liveQuery) run(irreversibleStartBlock bstream.BlockRef, headDelayTolerance uint64, streamSend func(*pb.SearchMatch) error) error {

	// irreversibleStartBlock is the lowest block number that we can serve. It is always
	// an irreversible block so we also consider it as "the LIB"
	// Note that it could be lower than the published tail block, within a 5s
	// grace period. Look at the TailManager to see that logic.
	// If the grace period is over and the LIB given here is higher than requested lower,
	// we will fail the boundary check, this is not a catastrophic failure
	// as the router will retry.

	_, _, irrNum, _, headNum, _ := q.searchPeer.HeadBlockPointers()
	if err := q.checkBoundaries(irreversibleStartBlock.Num(), irrNum, headNum, headDelayTolerance); err != nil {
		return err
	}

	go q.launchAggregator(streamSend)

	if q.request.Descending {
		// TODO: do we mark the `LiveBackend` as being `Ready` only when we've seen a few IRR blocks pass by?
		if err := q.runBackwardQuery(irreversibleStartBlock); err != nil {
			return err
		}

	} else {
		if err := q.runForwardQuery(irreversibleStartBlock); err != nil {
			// don't decorate, or else you risk fiddling with the OutOfRange error
			return err
		}

		// TODO: DMESH: WARN: this will fail when querying ETH's block 0
		// FIXME: what does that even do? does the caller care? it'll fail itself if we report
		// `last-block-read` of `0`, and retry?
		if q.lastBlockRead == 0 {
			return fmt.Errorf("live query reports not reading a single block")
		}
	}

	close(q.incomingMatches)
	<-q.aggregatorDone

	return q.aggregatorError
}

func (q *liveQuery) launchAggregator(streamSend func(*pb.SearchMatch) error) {
	defer close(q.aggregatorDone)

	var trxCount int64
	for {
		select {
		case <-q.ctx.Done():
			return
		case match, ok := <-q.incomingMatches:
			if !ok {
				return
			}

			if match.BlockNum > q.request.HighBlockNum {
				q.aggregatorError = fmt.Errorf("received result (%d) over the requested high block num (%d)", match.BlockNum, q.request.HighBlockNum)
				return
			}
			if match.BlockNum < q.request.LowBlockNum {
				continue // we might be in a fork. In this case, we will not undo above the requested start block num.
				// TODO: see what we would do with a cursor! maybe the router will give us a lowblocknum based on the cursor, so it wants us to go back to the LIB ?
			}

			trxCount++

			err := streamSend(match)
			if err != nil {
				q.zlog.Debug("failed sending payload, connection closed?", zap.Error(err))
				return
			}
		}
	}
}

func (q *liveQuery) isAggregatorDone() bool {
	select {
	case <-q.aggregatorDone:
		return true
	default:
	}
	return false
}

func toVirtualHead(withReversible bool, libNum uint64, head uint64) uint64 {
	if withReversible {
		return head
	}
	return libNum
}
