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
	"fmt"
	"time"

	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/derr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type queryExecutor struct {
	ctx                  context.Context
	zlogger              *zap.Logger
	request              *pb.RouterRequest
	cur                  *cursor
	cursorGate           *cursorGate
	queryRange           *QueryRange
	streamSend           func(*pb.SearchMatch) error
	trailer              metadata.MD
	trxCount             int64
	planner              Planner
	backendClientFactory backendClientFactory
	backendQueryFactory  backendQueryFactory
	lastBlockReceived    uint64

	limitReached    bool
	incompleteRange bool
}

func newQueryExecutor(ctx context.Context, req *pb.RouterRequest, planner Planner, cur *cursor, qRange *QueryRange, logger *zap.Logger, backendCliFactory backendClientFactory, backendQuFactory backendQueryFactory, streamSend func(*pb.SearchMatch) error) *queryExecutor {
	q := &queryExecutor{
		ctx:                  ctx,
		zlogger:              logger,
		request:              req,
		queryRange:           qRange,
		cur:                  cur,
		cursorGate:           NewCursorGate(cur, req.Descending),
		streamSend:           streamSend,
		trailer:              metadata.New(nil),
		planner:              planner,
		backendClientFactory: backendCliFactory,
		backendQueryFactory:  backendQuFactory,
	}

	return q
}

func (q *queryExecutor) Err() error {
	return q.ctx.Err()
}

func (q *queryExecutor) Query() error {

	// only one will move based on the queries direction
	movingLowBlockNum := q.queryRange.lowBlockNum
	movingHighBlockNum := q.queryRange.highBlockNum

	retryCount := 0
	retryBackoffs := []int{50, 200, 500, 1000, 1500}
	retryMax := len(retryBackoffs)

	for {
		targetPeer := q.planner.NextPeer(movingLowBlockNum, movingHighBlockNum, q.request.Descending, q.request.WithReversible)
		if targetPeer == nil {
			if retryCount >= retryMax {
				q.zlogger.Info("cannot get next target peer from planner",
					zap.Uint64("low_block_num", movingLowBlockNum),
					zap.Uint64("high_block_num", movingHighBlockNum),
					zap.Bool("decending", q.request.Descending),
					zap.Bool("with_reversible", q.request.WithReversible),
				)
				q.incompleteRange = true
				return fmt.Errorf("Internal server error")
			}
			q.zlogger.Debug("cannot get next target peer from planner, will retry",
				zap.Int("retry_count", retryCount),
				zap.Int("retry_max", retryMax),
			)
			time.Sleep(time.Duration(retryBackoffs[retryCount]) * time.Millisecond)
			retryCount++
			continue
		}

		backendRequest := q.createBackendQuery(targetPeer)

		q.zlogger.Info("running backend query request",
			zap.String("backend_addr", targetPeer.Addr),
			zap.Uint64("low_block_num", targetPeer.LowBlockNum),
			zap.Uint64("high_block_num", targetPeer.HighBlockNum),
			zap.Bool("serves_reversible", targetPeer.ServesReversible),
			zap.Any("backend_request", backendRequest))

		backendClient := q.backendClientFactory(targetPeer)

		backendQuery := q.backendQueryFactory(backendClient, backendRequest)

		err := backendQuery.run(q.ctx, q.zlogger, q.senderFilter)

		if err != nil {
			q.zlogger.Warn("backend query ran with error",
				zap.String("backend_addr", targetPeer.Addr),
				zap.Int64("last_block_read", backendQuery.LastBlockRead),
				zap.Int64("cumulative_matches_count", q.trxCount),
				zap.Error(err),
			)
			if err == ContextCanceled || q.ctx.Err() != nil { // our own ctx done should always cover the contextCanceled error upstream
				return nil
			}

			if err == LimitReached {
				return nil
			}

			if q.trxCount > 0 {
				q.zlogger.Error("backend error but results where already returned ",
					zap.Int64("trx_count", q.trxCount),
					zap.Error(err))
				return fmt.Errorf("Internal server error")
			}

			if retryCount >= retryMax {
				q.zlogger.Error("failed repeated reach out backing nodes", zap.Error(err))
				return fmt.Errorf("Internal server error")
			}

			time.Sleep(time.Duration(retryBackoffs[retryCount]) * time.Millisecond)
			retryCount++
			q.zlogger.Warn("backend query returned error, router will retry",
				zap.String("backend_addr", targetPeer.Addr),
				zap.Int("retry_count", retryCount),
				zap.Int("retry_max", retryMax),
				zap.Uint64("last_block_received", q.lastBlockReceived),
				zap.Error(err))
			continue

			// on the live when we disconnect forward, maybe we are in a firk, we don't want a retry in the live forward if the suer got one result
			// the reason why is because you could have been in a fork

		}
		q.zlogger.Debug("backend query ran",
			zap.String("backend_addr", targetPeer.Addr),
			zap.Int("retry_count", retryCount),
			zap.Int("retry_max", retryMax),
			zap.Uint64("last_block_received", q.lastBlockReceived),
		)

		if backendQuery.LastBlockRead == -1 {
			q.zlogger.Warn("backend query return no block read", zap.String("backend_addr", targetPeer.Addr))
			continue
		}

		lastBlockRead := uint64(backendQuery.LastBlockRead)

		if q.request.Descending {
			if lastBlockRead == q.queryRange.lowBlockNum {
				return nil
			} else if lastBlockRead < q.queryRange.lowBlockNum {
				q.zlogger.Error("descending request queried outside of lower query bound",
					zap.Uint64("query_low_block_num", q.queryRange.lowBlockNum),
					zap.Uint64("query_high_block_num", q.queryRange.highBlockNum),
					zap.Uint64("moving_high_block_num", movingHighBlockNum),
					zap.Uint64("backend_request_low_block_num", backendRequest.LowBlockNum),
					zap.Uint64("backend_request_high_block_num", backendRequest.HighBlockNum),
					zap.Uint64("backend_last_block_read", lastBlockRead))
				return fmt.Errorf("descending request queried outside of lower query bound")
			}
			movingHighBlockNum = lastBlockRead - 1
		} else {
			if targetPeer.ServesReversible {
				// you just finished the live backend
				if lastBlockRead < q.queryRange.highBlockNum {
					q.incompleteRange = true
				}
				// End the loop when the query is ascending and you have completed the last LiveBackend
				return nil
			}

			if lastBlockRead == q.queryRange.highBlockNum {
				return nil
			} else if lastBlockRead > q.queryRange.highBlockNum {
				q.zlogger.Error("ascending request queried outside of upper query bound",
					zap.Uint64("query_low_block_num", q.queryRange.lowBlockNum),
					zap.Uint64("query_high_block_num", q.queryRange.highBlockNum),
					zap.Uint64("moving_low_block_num", movingLowBlockNum),
					zap.Uint64("backend_request_low_block_num", backendRequest.LowBlockNum),
					zap.Uint64("backend_request_high_block_num", backendRequest.HighBlockNum),
					zap.Uint64("backend_last_block_read", lastBlockRead))
				return fmt.Errorf("ascending request queried outside of lower query bound")
			}
			movingLowBlockNum = lastBlockRead + 1
		}
	}
}

func (q *queryExecutor) createBackendQuery(targetPeerRange *PeerRange) *pb.BackendRequest {
	backendRequest := &pb.BackendRequest{
		Query:        q.request.Query,
		LowBlockNum:  targetPeerRange.LowBlockNum,
		HighBlockNum: targetPeerRange.HighBlockNum,
		Descending:   q.request.Descending,
	}

	if !q.request.Descending && targetPeerRange.ServesReversible && q.queryRange.mode == pb.RouterRequest_PAGINATED {
		backendRequest.StopAtVirtualHead = true
		zlog.Debug("setting stopAtVirtualHead")
	} else {
		zlog.Debug("NOT setting stopAtVirtualHead", zap.Bool("descending", q.request.Descending), zap.Bool("serves_reversible", targetPeerRange.ServesReversible), zap.Any("queryrange_mode", q.queryRange.mode))
	}

	if targetPeerRange.ServesReversible {
		backendRequest.WithReversible = q.request.WithReversible
		backendRequest.LiveMarkerInterval = q.request.LiveMarkerInterval

		if q.cur != nil {
			backendRequest.NavigateFromBlockID = q.cur.headBlockID
			backendRequest.NavigateFromBlockNum = q.cur.blockNum
		}
	}
	return backendRequest
}

func (q *queryExecutor) senderFilter(match *pb.SearchMatch) error {

	if !within(match.BlockNum, q.queryRange.lowBlockNum, q.queryRange.highBlockNum) {
		q.zlogger.Warn("received matched outside requested query range",
			zap.Reflect("query_range", q.queryRange),
			zap.Reflect("search_match", match))
		return derr.Statusf(codes.OutOfRange, "received matched outside requested query range [%d,%d]f", q.queryRange.lowBlockNum, q.queryRange.highBlockNum)
	}

	// do not send matches if you have not passed the cursor gate
	pass := q.cursorGate.passed(match.BlockNum, match.TrxIdPrefix)
	if !pass {
		return nil
	}

	q.trxCount++
	q.lastBlockReceived = match.BlockNum

	err := q.streamSend(match)
	if err != nil {
		zlog.Debug("failed sending payload, connection closed?", zap.Error(err))
		return err
	}

	// Assumption here: we tell your the limit is reached but we will NOT tell you
	// there are more coming.  We don't know, and can't know, in whatever direction.
	if q.request.Limit != 0 && q.trxCount >= q.request.Limit {
		q.limitReached = true
		return LimitReached
	}
	return nil
}
