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
	"github.com/streamingfast/derr"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/streamingfast/search"
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

type IndexedBlock struct {
	Idx *search.SingleIndex
	Blk *bstream.Block
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
func (q *LiveQuery) processBlocks(backwardBlocks []*IndexedBlock) (err error) {
	// Flip block direction, and pipe the refs
	for i := len(backwardBlocks) - 1; i >= 0; i-- {
		idxBlk := backwardBlocks[i]
		if idxBlk.Blk.Num() < q.Request.LowBlockNum || idxBlk.Blk.Num() > q.Request.HighBlockNum {
			zlog.Warn("curiously going over a block out of request bounds", zap.Uint64("idx_blk_num", idxBlk.Blk.Num()))
			continue
		}
		err = q.ProcessSingleBlocks(q.Ctx, idxBlk, q.MatchCollector, q.IncomingMatches)
		if err != nil {
			return err
		}
	}
	return nil

}
func (q *LiveQuery) ProcessSingleBlocks(ctx context.Context, indexedBlock *IndexedBlock, matchCollector search.MatchCollector, incomingMatches chan *pb.SearchMatch) (err error) {
	// Flip block direction, and pipe the refs
	idx := indexedBlock.Idx
	blk := indexedBlock.Blk

	matches, err := search.RunSingleIndexQuery(ctx, true, 0, math.MaxUint32, matchCollector, q.BleveQuery, idx.Index, func() {}, nil)
	if err != nil {
		if err == context.Canceled {
			return derr.Status(codes.Canceled, "context canceled")
		}
		return fmt.Errorf("running single query: %s", err)
	}

	q.LastBlockRead = blk.Num()

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

func (q *LiveQuery) runBackwardQuery(firstBlockRef bstream.BlockRef) (err error) {

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
		backBlockNums = append(backBlockNums, blk.Blk.Num())
	}
	zlog.Debug("blocks found after running backward query", zap.Uint64("req_high_block_num", q.Request.HighBlockNum), zap.Uint64("req_low_block_num", q.Request.LowBlockNum), zap.Any("backward_block_nums", backBlockNums))

	return q.processBlocks(q.backwardBlocks)
}

func (q *LiveQuery) setupBackwardPipeline(firstBlockRef bstream.BlockRef) *forkable.Forkable {
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

		q.backwardBlocks = append(q.backwardBlocks, &IndexedBlock{
			Blk: blk,
			Idx: idx,
		})
		if blk.Num() >= q.Request.HighBlockNum {
			return search.ErrEndOfRange
		}

		return nil
	})

	postForkGate := bstream.NewBlockNumGate(q.Request.LowBlockNum, bstream.GateInclusive, handler)

	if firstBlockRef.ID() == "" && firstBlockRef.Num() != bstream.GetProtocolFirstStreamableBlock {
		zlog.Debug("firstBlockRef ID is not set on a live search call, this may affect performance of this search", zap.Uint64("first_block_ref_num", firstBlockRef.Num()))
	}
	options := []forkable.Option{
		forkable.WithInclusiveLIB(firstBlockRef),
		forkable.WithFilters(forkable.StepNew | forkable.StepUndo),
	}

	if q.Request.NavigateFromBlockID != "" {
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
		options = append(options, forkable.EnsureBlockFlows(bstream.NewBlockRef(q.Request.NavigateFromBlockID, q.Request.NavigateFromBlockNum)))
	}
	forkableHandler := forkable.New(postForkGate, options...)

	return forkableHandler

}
