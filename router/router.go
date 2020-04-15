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
	"net"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/dfuse-io/logging"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Router struct {
	*shutter.Shutter

	blockIDClient      pbblockmeta.BlockIDClient
	forksClient        pbblockmeta.ForksClient
	dmeshClient        dmeshClient.SearchClient
	ready              atomic.Bool
	headDelayTolerance uint64
	libDelayTolerance  uint64
	enableRetry        bool
}

func New(dmeshClient dmeshClient.SearchClient, headDelayTolerance uint64, libDelayTolerance uint64, blockIDClient pbblockmeta.BlockIDClient, forksClient pbblockmeta.ForksClient, enableRetry bool) *Router {
	return &Router{
		Shutter:            shutter.New(),
		forksClient:        forksClient,
		blockIDClient:      blockIDClient,
		dmeshClient:        dmeshClient,
		headDelayTolerance: headDelayTolerance,
		libDelayTolerance:  libDelayTolerance,
		enableRetry:        enableRetry,
	}
}

func checkCursorStillValid(ctx context.Context, cur *cursor, libnum uint64, blockmeta pbblockmeta.BlockIDClient) error {
	zlogger := logging.Logger(ctx, zlog)
	if cur == nil {
		return nil
	}
	if cur.headBlockID == "" {
		return nil
	}
	if cur.blockNum > libnum {
		return nil
	}
	irrBlk, err := search.GetIrreversibleBlock(blockmeta, cur.blockNum, ctx, 5)
	if err != nil {
		zlogger.Error("connecting to blockmeta to get irrBlk", zap.Uint64("cursor_block_num", cur.blockNum), zap.Error(err))
		return fmt.Errorf("internal server error while validating cursor")
	}
	if irrBlk.ID() == cur.headBlockID {
		return nil
	}

	return fmt.Errorf("cursor is not valid anymore: it points to block %s, which was forked out. The correct block ID at this height is %s", cur.headBlockID, irrBlk.ID())
}

func (r *Router) StreamMatches(req *pb.RouterRequest, stream pb.Router_StreamMatchesServer) error {
	ctx := stream.Context()
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Info("routing active query",
		zap.Reflect("request", req),
		zap.Uint64("head_delay_tolerance", r.headDelayTolerance),
		zap.Uint64("lib_delay_tolerance", r.libDelayTolerance),
	)

	if !r.ready.Load() {
		zlog.Info("request received while router is not ready")
		return status.Errorf(codes.Unavailable, "search is currently unavailable, try again shortly.")
	}

	_, err := search.NewParsedQuery(req.Query)
	if err != nil {
		zlogger.Debug("invalid parsed query", zap.String("query", req.Query), zap.Error(err))
		return err
	}

	headBlock, irrBlock := getSearchHighestHeadInfo(r.dmeshClient.Peers())
	headBlockNumber.SetUint64(headBlock)
	metrics.IRRBlockNumber.SetUint64(irrBlock)
	zlogger.Debug("retrieve networks state", zap.Uint64("irr_block", irrBlock), zap.Uint64("head_block", headBlock))

	cur, err := NewCursorFromString(req.Cursor)
	if err != nil {
		zlogger.Warn("invalid cursor", zap.String("raw_cursor", req.Cursor), zap.Error(err))
		return status.Errorf(codes.InvalidArgument, err.Error())
	}

	resolvedForkTrxCount := int64(0)
	cursorErr := checkCursorStillValid(ctx, cur, irrBlock, r.blockIDClient)
	if cursorErr != nil {
		if req.Descending {
			zlogger.Warn("invalid cursor according to LIB and descending", zap.String("raw_cursor", req.Cursor), zap.Error(cursorErr))
			return status.Errorf(codes.InvalidArgument, cursorErr.Error())
		}

		zlogger.Debug("attempt to navigate fork", zap.String("cursor_error", cursorErr.Error()), zap.String("raw_cursor", req.Cursor))
		forkLogger := zlogger.With(zap.String("cursor_error", cursorErr.Error()), zap.String("raw_cursor", req.Cursor))
		forkExec := newForkExecutor(ctx, r.forksClient, r.dmeshClient.Peers, forkLogger, cur, req.Query, stream.Send)
		lowestBlockNum, sentTrxCount, err := forkExec.query()
		if err != nil {
			if err == InvalidArgument {
				return status.Errorf(codes.InvalidArgument, cursorErr.Error())
			}

			if err == ContextCanceled {
				return derr.Status(codes.Canceled, "context canceled")
			}

			return err
		}
		cur = nil
		req.LowBlockNum = lowestBlockNum
		resolvedForkTrxCount = sentTrxCount
	}

	qRange, err := newQueryRange(req, cur, headBlock, irrBlock, r.headDelayTolerance, r.libDelayTolerance)
	if err != nil {
		zlogger.Error("invalid range",
			zap.Reflect("router_request", req),
			zap.Uint64("head_block", headBlock),
			zap.Uint64("irr_block", irrBlock),
			zap.Error(err),
		)
		return err
	}
	zlogger.Debug("formatted query range",
		zap.Uint64("low_block_num", qRange.lowBlockNum),
		zap.Uint64("high_block_num", qRange.highBlockNum),
		zap.String("mode", qRange.mode.String()),
	)

	qRange = adjustQueryRange(qRange)
	zlogger.Info("adjusted query range",
		zap.Uint64("low_block_num", qRange.lowBlockNum),
		zap.Uint64("high_block_num", qRange.highBlockNum),
		zap.String("mode", qRange.mode.String()),
	)

	metrics.TotalRequestCount.Inc()
	metrics.InflightRequestCount.Inc()
	defer metrics.InflightRequestCount.Dec()

	trailer := metadata.New(nil)
	defer stream.SetTrailer(trailer)

	planner := NewDmeshPlanner(r.dmeshClient.Peers, r.headDelayTolerance)

	q := newQueryExecutor(ctx, req, planner, cur, qRange, zlogger, newBackendClient, newBackendQuery, stream.Send)
	if resolvedForkTrxCount != 0 {
		q.trxCount = resolvedForkTrxCount
	}
	defer stream.SetTrailer(q.trailer) // set trailer before canceling context (thus, after in code)

	if err := q.Query(); err != nil {
		metrics.ErrorRequestCount.Inc()
		return err
	}

	if q.Err() != nil {
		return nil
	}

	setComplete(trailer, q)

	return nil
}

func (r *Router) startServer(listenAddr string) {
	// gRPC
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		r.Shutter.Shutdown(fmt.Errorf("failed listening grpc %q: %w", listenAddr, err))
		return
	}

	s := dgrpc.NewServer(dgrpc.WithLogger(zlog))
	go metrics.ServeMetrics()
	pb.RegisterRouterServer(s, r)
	pbhealth.RegisterHealthServer(s, r)

	go func() {
		zlog.Info("listening & serving gRPC content", zap.String("grpc_listen_addr", listenAddr))
		if err := s.Serve(lis); err != nil {
			r.Shutter.Shutdown(fmt.Errorf("error on gs.Serve: %w", err))
		}
	}()
}

func (r *Router) Launch(grpcListenAddr string) {
	r.startServer(grpcListenAddr)
	go r.setRouterAvailability()
	select {
	case <-r.Terminating():
		if err := r.Err(); err != nil {
			zlog.Error("router terminated with error", zap.Error(err))
		} else {
			zlog.Info("router terminated")

		}
	}

}

func getSearchHighestHeadInfo(peers []*dmesh.SearchPeer) (headBlock uint64, irrBlock uint64) {
	readPeers := getReadyPeers(peers)
	for _, peer := range readPeers {
		if peer.HeadBlock > headBlock {
			headBlock = peer.HeadBlock
		}
		if peer.IrrBlock > irrBlock {
			irrBlock = peer.IrrBlock
		}
	}
	return headBlock, irrBlock
}

func setComplete(trailer metadata.MD, sharder *queryExecutor) {
	rangeCompleted := !sharder.incompleteRange && !sharder.limitReached

	if rangeCompleted {
		trailer.Set("range-completed", "true")
	}
}

func adjustQueryRange(qr *QueryRange) *QueryRange {
	if qr.lowBlockNum < bstream.GetProtocolFirstBlock {
		zlog.Debug("adjusting query range for protocol",
			zap.Uint64("low_block_num", qr.lowBlockNum),
			zap.Uint64("adjusted_low_block_num", bstream.GetProtocolFirstBlock),
		)
		qr.lowBlockNum = bstream.GetProtocolFirstBlock
	}
	return qr
}
