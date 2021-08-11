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
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/derr"
	pbsearch "github.com/streamingfast/pbgo/dfuse/search/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryRange struct {
	lowBlockNum  uint64
	highBlockNum uint64
	mode         pbsearch.RouterRequest_Mode
}

const UnboundedInt int64 = int64(9_000_000_000_000_000_000)

/*
	Goal: To normalize and resolve a specific request into absolute bound that will be used in the planner algorithm
	Notes:
		* We use UnboundedInt to perform unbounded since that block number will never be reached
	Desired behavior:
		__ascending queries__
		[L,H]			->		[ L, H ]
		[-L,-H]			-> 		[ (VirH-L), (VirH-H) ]
		[None,None]		-> 		[ 0, UnboundedInt]
		[None,H]		->		[ 0, H ]
		[L,None]		->		[ L, UnboundedInt]
		[None,-H]		->		[ 0, (VirH-H) ]
		[-L,None]		->		[ (VirH-L), UnboundedInt]

		__descending queries__
		[None,None]		-> 		ERROR cannot descend from infinity you need to use `-1` to reference the virtual head
		[L,H]			->		[ L, H ]
		[None,H]		->		[ 0, H ]  // 0 can be replaced by 'truncation LowBlockNum'
		[L,None]		-> 		ERROR cannot descend from infinity you need to use `-1` to reference the virtual head
		[-L,-H]			-> 		[ (VirH-L), (VirH-H) ]
		[None,-H]		->		[ 0, (VirH-H) ] // 0 can be replaced by 'truncation LowBlockNum'
		[-L,None]		->		ERROR cannot descend from infinity you need to use `-1` to reference the virtual head

	Note
	• In paginated searches forward, we refuse `highBlockNum` that would be beyond the HEAD block with an immediate error: we tell them to use `-1` to reach the end of the chain
	• In paginated _and_ streaming search, backward, we refuse `highBlockNum` going beyond HEAD with: `highBlockNum goes beyond HEAD block, use 0 to start from the HEAD`.
   	• In paginated search, we refuse a `lowBlockNum` that would be beyond the HEAD block. For forward search, we tell them: `lowBlocknum beyond HEAD block, use negative values to start near the head of the chain`; for backward search, we tell them: `lowBlockNum beyond HEAD block`
*/

func absTruncLowBlockNum(head uint64, truncationLowBlockNum int64) (uint64, error) {
	//uint64(adjustNegativeValues(truncationLowBlockNum, int64(head)))
	if truncationLowBlockNum < 0 {
		headMinusTrunc := int64(head) + truncationLowBlockNum
		if headMinusTrunc < 0 {
			return 0, fmt.Errorf("HEAD is below negative truncationLowBlockNum")
		}
		return uint64(headMinusTrunc), nil
	}
	return uint64(truncationLowBlockNum), nil
}

func newQueryRange(req *pbsearch.RouterRequest, c *cursor, head uint64, irr uint64, headDelayTolerance uint64, libDelayTolerance uint64, truncationLowBlockNum int64) (qr *QueryRange, err error) {
	absoluteTruncationLowBlockNum, err := absTruncLowBlockNum(head, truncationLowBlockNum)
	if err != nil {
		return nil, err
	}

	if req.UseLegacyBoundaries {
		qr, err = parseLegacyRequest(req, head, irr, headDelayTolerance, libDelayTolerance, absoluteTruncationLowBlockNum, c)
	} else {
		qr, err = parseRequest(req, head, irr, headDelayTolerance, libDelayTolerance, absoluteTruncationLowBlockNum, c)
	}
	if err != nil {
		return nil, err
	}
	return qr, nil

}

func applyCursor(descending bool, qr *QueryRange, cursor *cursor, absoluteTruncationLowBlockNum uint64) (*QueryRange, error) {
	if cursor == nil {
		return qr, nil
	}

	if cursor.blockNum < qr.lowBlockNum {
		if descending {
			return nil, status.Errorf(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (%d) is out of requested block range [%d-%d].", cursor.blockNum, qr.lowBlockNum, qr.highBlockNum)
		}
	}

	if cursor.blockNum < absoluteTruncationLowBlockNum {
		return nil, status.Errorf(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (%d) is lower than the lowest block served by this endpoint (%d).", cursor.blockNum, absoluteTruncationLowBlockNum)
	}

	if cursor.blockNum > qr.highBlockNum {
		return nil, status.Errorf(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (%d) is out of requested block range [%d-%d].", cursor.blockNum, qr.lowBlockNum, qr.highBlockNum)
	}

	if descending && cursor.blockNum != 0 {
		qr.highBlockNum = cursor.blockNum
	}

	if !descending && cursor.blockNum != 0 {
		qr.lowBlockNum = cursor.blockNum
	}
	return qr, nil
}

func parseLegacyRequest(req *pbsearch.RouterRequest, head uint64, lib uint64, headDelayTolerance uint64, libDelayTolerance uint64, absoluteTruncationLowBlockNum uint64, cursor *cursor) (*QueryRange, error) {
	var lowBlkNum int64
	var highBlkNum int64

	if req.BlockCount == 0 {
		return nil, derr.Statusf(codes.InvalidArgument, "invalid block count: cannot be zero")
	}

	if req.WithReversible {
		if req.StartBlock > (head + headDelayTolerance) {
			return nil, derr.Statusf(codes.InvalidArgument, "invalid start block num: goes beyond HEAD block, use `0` to follow the HEAD (requested: %d, head: %d)", req.StartBlock, head)
		}
	} else {
		if req.StartBlock > (lib + libDelayTolerance) {
			return nil, derr.Statusf(codes.InvalidArgument, "invalid start block num: goes beyond LIB, use `0` to follow LIB (requested: %d, lib: %d)", req.StartBlock, lib)
		}
	}

	virtualHead := withVirtualHead(req, head, lib)

	if req.Descending {
		if req.StartBlock == 0 {
			highBlkNum = virtualHead
			if highBlkNum < int64(absoluteTruncationLowBlockNum) {
				return nil, derr.Statusf(codes.InvalidArgument, "invalid start block: HEAD [%d] is lower than the lowest block served by this endpoint [%d]", virtualHead, absoluteTruncationLowBlockNum)
			}
		} else {
			highBlkNum = int64(req.StartBlock)
			if req.StartBlock < absoluteTruncationLowBlockNum {
				return nil, derr.Statusf(codes.InvalidArgument, "invalid start block: %d is lower than the lowest block served by this endpoint [%d]", req.StartBlock, absoluteTruncationLowBlockNum)
			}

		}

		lowBlkNum = highBlkNum - int64(req.BlockCount)
		if lowBlkNum <= int64(absoluteTruncationLowBlockNum) { // we tolerate super high block_count
			lowBlkNum = int64(absoluteTruncationLowBlockNum)
		}
		if lowBlkNum < 1 {
			lowBlkNum = 1
		}
	} else { // ASCENDING
		switch {
		case req.StartBlock == 0:
			lowBlkNum = 1 // original behaviour, contrary to what the docs say. will not affect block count (0->100 == 1->100)
			if absoluteTruncationLowBlockNum != 0 {
				lowBlkNum = int64(absoluteTruncationLowBlockNum)
			}
		case req.StartBlock < 0:
			lowBlkNum = virtualHead + int64(req.StartBlock)

			if lowBlkNum < 1 {
				return nil, derr.Statusf(codes.InvalidArgument, "invalid start block: (head or lib - %d) is lower than first block (head: %d, lib: %d)", -req.StartBlock, head, lib)
			}
			if lowBlkNum < int64(absoluteTruncationLowBlockNum) {
				return nil, derr.Statusf(codes.InvalidArgument, "invalid start block: (head or lib - %d) is lower than the lowest block served by this endpoint [%d] (head: %d, lib: %d)", lowBlkNum, absoluteTruncationLowBlockNum, head, lib)
			}

		case req.StartBlock > 0:
			if req.StartBlock < absoluteTruncationLowBlockNum && cursor == nil { // we do not validate lowblocknum on ascending requests with cursor, the applyCursor function will do it
				return nil, derr.Statusf(codes.InvalidArgument, "invalid start block: %d is lower than the lowest block served by this endpoint [%d]", req.StartBlock, absoluteTruncationLowBlockNum)
			}
			lowBlkNum = int64(req.StartBlock)
		}

		highBlkNum = lowBlkNum + int64(req.BlockCount)

		if highBlkNum > UnboundedInt { // here, we allow going over HEAD or LIB
			highBlkNum = UnboundedInt // legacy max value
		}
	}

	qr := &QueryRange{
		lowBlockNum:  uint64(lowBlkNum),
		highBlockNum: uint64(highBlkNum),
		mode:         pbsearch.RouterRequest_PAGINATED, // legacy is always paginated, so we can keep a highblocknum passed HEAD
	}
	return applyCursor(req.Descending, qr, cursor, absoluteTruncationLowBlockNum)
}

func parseRequest(req *pbsearch.RouterRequest, head uint64, lib uint64, headDelayTolerance uint64, libDelayTolerance uint64, absoluteTruncationLowBlockNum uint64, cursor *cursor) (*QueryRange, error) {

	// Virtual head is either HEAD or LIB
	tolerance := int64(libDelayTolerance)
	if req.WithReversible {
		tolerance = int64(headDelayTolerance)
	}

	virtualHead := withVirtualHead(req, head, lib)
	highBlkNum := adjustNegativeValues(req.HighBlockNum, virtualHead)
	lowBlkNum := adjustNegativeValues(req.LowBlockNum, virtualHead)

	if req.LowBlockUnbounded {
		lowBlkNum = int64(absoluteTruncationLowBlockNum)
	}

	if req.HighBlockUnbounded {
		if req.Descending {
			highBlkNum = virtualHead // don't break existing queries
		} else {
			highBlkNum = UnboundedInt
		}
	}

	highBlockNumInFuture := highBlkNum > (virtualHead + tolerance) // we give ourselves a reasonable threshold.. so account for a big of lag within the network
	descendFromFuture := req.Descending && highBlockNumInFuture

	if descendFromFuture {
		if req.WithReversible {
			// You cannot descend from an unbounded high block [52632,UNBOUND] DESC does not make sense
			return nil, derr.Statusf(codes.InvalidArgument, "invalid high block num: it goes beyond the current head block, use `-1` for to follow the HEAD (requested: %d, head: %d)", req.HighBlockNum, virtualHead)
		} else {
			// You cannot descend from an unbounded high block [52632,UNBOUND] DESC does not make sense especially when you get only irreversible
			return nil, derr.Statusf(codes.InvalidArgument, "invalid high block num: it goes beyond the current last irreversible block, use `-1` to follow the LIB (requested: %d, lib: %d)", req.HighBlockNum, virtualHead)
		}
	}

	lowBlockNumInFuture := lowBlkNum > (virtualHead + tolerance)

	if lowBlockNumInFuture {
		return nil, derr.Statusf(codes.InvalidArgument, "invalid low block num: it goes beyond the current head block, use `-1` for to follow the HEAD (requested: %d, head: %d)", req.HighBlockNum, virtualHead)
	}

	if lowBlkNum < 0 {
		return nil, derr.Statusf(codes.InvalidArgument, "invalid low block num: goes beyond first block, value was %d", req.LowBlockNum)
	}

	if lowBlkNum < int64(absoluteTruncationLowBlockNum) {
		if !req.Descending && cursor == nil { // we do not validate lowblocknum on ascending requests with cursor, the applyCursor function will do it
			return nil, derr.Statusf(codes.InvalidArgument, "invalid low block num on ascending request: %d is lower than the lowest block served by this endpoint [%d]", lowBlkNum, absoluteTruncationLowBlockNum)
		}
		if req.Descending && !req.LowBlockUnbounded && lowBlkNum > int64(bstream.GetProtocolFirstStreamableBlock) {
			return nil, derr.Statusf(codes.InvalidArgument, "invalid low block num on descending request: %d is lower than the lowest block served by this endpoint [%d]", lowBlkNum, absoluteTruncationLowBlockNum)
		}
		lowBlkNum = int64(absoluteTruncationLowBlockNum)
	}

	if highBlkNum < 0 {
		return nil, derr.Statusf(codes.InvalidArgument, "invalid high block num: goes beyond first block, value was %d", req.HighBlockNum)
	}

	if highBlkNum < lowBlkNum {
		return nil, derr.Statusf(codes.InvalidArgument, "invalid high block num: goes beyond low block, value was %d", req.HighBlockNum)
	}

	qr := &QueryRange{
		lowBlockNum:  uint64(lowBlkNum),
		highBlockNum: uint64(highBlkNum),
		mode:         req.Mode,
	}
	return applyCursor(req.Descending, qr, cursor, absoluteTruncationLowBlockNum)
}

func adjustNegativeValues(number int64, head int64) int64 {
	if number < 0 {
		return head + number + 1
	}
	return number
}

func withVirtualHead(r *pbsearch.RouterRequest, head uint64, libNum uint64) int64 {
	if r.WithReversible {
		return int64(head)
	}
	return int64(libNum)
}
