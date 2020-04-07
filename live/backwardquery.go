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
	"math"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/derr"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/search"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

//
// BackwardLiveQuery
//

// type BackwardLiveQuery struct {
// 	liveQuery *liveQuery

// 	archiveBlockRef bstream.BlockRef

// 	src bstream.Source

// 	ctx    context.Context
// 	blocks []*indexedBlock
// }

type indexedBlock struct {
	idx *search.SingleIndex
	blk *bstream.Block
}

// func (q *liveQuery) newBackwardLiveQuery(
// 	firstBlockRef bstream.BlockRef,
// 	lowBlockNum, highBlockNum uint64,
// ) *BackwardLiveQuery {
// 	return &BackwardLiveQuery{
// 		liveQuery:     q,
// 		firstBlockRef: firstBlockRef,
// 		highBlockNum:  highBlockNum,
// 		lowBlockNum:   lowBlockNum,
// 	}
// }
func (q *liveQuery) processBlocks(backwardBlocks []*indexedBlock) (err error) {
	// Flip block direction, and pipe the refs
	for i := len(backwardBlocks) - 1; i >= 0; i-- {
		idxBlk := backwardBlocks[i]
		if idxBlk.blk.Num() < q.request.LowBlockNum || idxBlk.blk.Num() > q.request.HighBlockNum {
			zlog.Warn("curiously going over a block out of request bounds", zap.Uint64("idx_blk_num", idxBlk.blk.Num()))
			continue
		}
		err = q.processSingleBlocks(q.ctx, idxBlk, q.matchCollector, q.incomingMatches)
		if err != nil {
			return err
		}
	}
	return nil

}
func (q *liveQuery) processSingleBlocks(ctx context.Context, indexedBlock *indexedBlock, matchCollector search.MatchCollector, incomingMatches chan *pb.SearchMatch) (err error) {
	// Flip block direction, and pipe the refs
	idx := indexedBlock.idx
	blk := indexedBlock.blk

	matches, err := search.RunSingleIndexQuery(ctx, true, 0, math.MaxUint32, matchCollector, q.bleveQuery, idx.Index, func() {}, nil)
	if err != nil {
		if err == context.Canceled {
			return derr.Status(codes.Canceled, "context canceled")
		}
		return fmt.Errorf("running single query: %s", err)
	}

	q.lastBlockRead = blk.Num()

	for _, match := range matches {
		matchProto, err := liveSearchMatchToProto(blk, blk.LIBNum(), false, match)
		if err != nil {
			return fmt.Errorf("unable to create search match proto: %s", err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return derr.Status(codes.Canceled, "context canceled")
			}
			return ctx.Err()
		case <-q.aggregatorDone:
			return nil
		case incomingMatches <- matchProto:
		}
	}
	return nil

}

func (q *liveQuery) runBackwardQuery(firstBlockRef bstream.BlockRef) (err error) {

	handler := q.setupBackwardPipeline(firstBlockRef)

	source, err := q.sourceFromBlockNumFunc(firstBlockRef.Num(), handler)
	if err != nil {
		return fmt.Errorf("backward query failed to obtain hub source: %s", err)
	}

	source.Run()

	if err := source.Err(); err != nil {
		if err != search.ErrEndOfRange /* in tandem with the bstream.Handler below */ {
			return err
		}
	}
	// TODO remove this debugging computation
	var backBlockNums []uint64
	for _, blk := range q.backwardBlocks {
		backBlockNums = append(backBlockNums, blk.blk.Num())
	}
	zlog.Debug("blocks found after running backward query", zap.Uint64("req_high_block_num", q.request.HighBlockNum), zap.Uint64("req_low_block_num", q.request.LowBlockNum), zap.Any("backward_block_nums", backBlockNums))

	return q.processBlocks(q.backwardBlocks)
}

func (q *liveQuery) setupBackwardPipeline(firstBlockRef bstream.BlockRef) *forkable.Forkable {
	handler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		if q.isAggregatorDone() {
			return derr.Status(codes.Canceled, "context canceled")
		}

		fObj := obj.(*forkable.ForkableObject)

		if fObj.Step == forkable.StepUndo {
			if len(q.backwardBlocks) == 0 {
				return nil
			}

			q.backwardBlocks = q.backwardBlocks[:len(q.backwardBlocks)-1]
			return nil
		}
		if fObj.Step != forkable.StepNew {
			panic("this handler only supports Undo and New steps")
		}

		idx := fObj.Obj.(*search.SingleIndex)

		q.backwardBlocks = append(q.backwardBlocks, &indexedBlock{
			blk: blk,
			idx: idx,
		})
		if blk.Num() >= q.request.HighBlockNum {
			return search.ErrEndOfRange
		}

		return nil
	})

	postForkGate := bstream.NewBlockNumGate(q.request.LowBlockNum, bstream.GateInclusive, handler)

	if firstBlockRef.ID() == "" {
		zlog.Warn("firstBlockRef ID not set")
	} else {
		zlog.Debug("creating forkable with LIB", zap.Uint64("firstBlockRefNum", firstBlockRef.Num()), zap.String("firstBlockRefID", firstBlockRef.ID()))
	}
	options := []forkable.Option{
		forkable.WithInclusiveLIB(firstBlockRef),
		forkable.WithFilters(forkable.StepNew | forkable.StepUndo),
	}

	if q.request.NavigateFromBlockID != "" {
		// TODO: WARN: if we set this (as we are navigating forks), it
		// means WE POSSIBLY CAN RECEIVE BLOCKS WITH A LOWER BLOCK NUM
		// than our lower boundary, especially since the effect of a
		// cursor is to make the `lowBlockNum` equal to the
		// cursorBlockNum..  This means that the `Router` needs to be
		// indulgent with its bound checks when processing the reply
		// from a `live backend` w/ ascending query & withReversible
		//
		// NOTE: the blockNumGate above should be fine, and always be
		// triggered, even though previous blocks are sent after,
		// because if the user was provided with a cursor, it meant
		// that that particular block had a match, which we should
		// also have in this situation, therefore tripping the
		// `blockNumGate`.
		options = append(options, forkable.EnsureBlockFlows(bstream.NewBlockRef(q.request.NavigateFromBlockID, q.request.NavigateFromBlockNum)))
	}
	forkableHandler := forkable.New(postForkGate, options...)

	return forkableHandler

}
