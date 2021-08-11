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

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/hub"
	pb "github.com/streamingfast/pbgo/dfuse/search/v1"
	"github.com/streamingfast/dmesh"
	"github.com/streamingfast/search"
	"go.uber.org/zap"
)

type LiveQuery struct {
	zlog       *zap.Logger
	searchPeer *dmesh.SearchPeer

	Ctx context.Context

	sourceFromBlockNumFunc func(startBlockNum uint64, handler bstream.Handler) (*hub.HubSource, error)
	MatchCollector         search.MatchCollector

	Request    *pb.BackendRequest
	BleveQuery *search.BleveQuery

	IncomingMatches chan *pb.SearchMatch
	aggregatorDone  chan struct{}
	aggregatorError error

	LastBlockRead uint64

	// fwd only
	LiveMarkerReached          bool
	LiveMarkerLastSentBlockNum uint64
	// backward only
	backwardBlocks []*IndexedBlock
}

func (b *LiveBackend) newLiveQuery(ctx context.Context, request *pb.BackendRequest, bquery *search.BleveQuery) *LiveQuery {
	q := &LiveQuery{
		sourceFromBlockNumFunc: b.hub.NewHubSourceFromBlockNum,
		MatchCollector:         b.matchCollector,
		searchPeer:             b.searchPeer,
		Ctx:                    ctx,
		zlog:                   zlog,
		Request:                request,
		IncomingMatches:        make(chan *pb.SearchMatch, 100),
		aggregatorDone:         make(chan struct{}),
		BleveQuery:             bquery,
	}

	return q
}

func (q *LiveQuery) checkBoundaries(first, irr, head, headDelayTolerance uint64) error {
	virtHead := toVirtualHead(q.Request.WithReversible, irr, head)

	if q.Request.Descending {
		if q.Request.HighBlockNum > virtHead+headDelayTolerance {
			return fmt.Errorf("descending high block num requested (%d) higher than available virtual head (%d) with head delay tolerance (%d)", q.Request.HighBlockNum, virtHead, headDelayTolerance)
		}

	} else {
		if q.Request.LowBlockNum < first {
			return fmt.Errorf("ascending requested lower boundary (%d) lower than available tail block (%d) on this node", q.Request.LowBlockNum, first)
		}

		if (virtHead + headDelayTolerance) < q.Request.LowBlockNum {
			return fmt.Errorf("ascending requested lower boundary (%d) higher than virtual head (%d) with head delay tolerance (%d)", q.Request.LowBlockNum, virtHead, headDelayTolerance)
		}

	}

	return nil
}

func (q *LiveQuery) run(irreversibleStartBlock bstream.BlockRef, headDelayTolerance uint64, streamSend func(*pb.SearchMatch) error) error {

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

	if q.Request.Descending {
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
		if q.LastBlockRead == 0 {
			return fmt.Errorf("live query reports not reading a single block")
		}
	}

	close(q.IncomingMatches)
	<-q.aggregatorDone

	return q.aggregatorError
}

func (q *LiveQuery) launchAggregator(streamSend func(*pb.SearchMatch) error) {
	defer close(q.aggregatorDone)

	var trxCount int64
	for {
		select {
		case <-q.Ctx.Done():
			return
		case match, ok := <-q.IncomingMatches:
			if !ok {
				return
			}

			if match.BlockNum > q.Request.HighBlockNum {
				q.aggregatorError = fmt.Errorf("received result (%d) over the requested high block num (%d)", match.BlockNum, q.Request.HighBlockNum)
				return
			}
			if match.BlockNum < q.Request.LowBlockNum {
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

func (q *LiveQuery) isAggregatorDone() bool {
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
