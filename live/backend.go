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
	"net"
	"time"

	"github.com/dfuse-io/bstream/hub"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dgrpc"
	"github.com/dfuse-io/logging"
	pbhead "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/dmesh"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search"
	"github.com/streamingfast/search/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type LiveBackend struct {
	*shutter.Shutter
	startBlock               uint64 // block at which the live router starts live-indexing, upon boot.
	nextTierBackendsBlockNum *atomic.Uint64
	hub                      *hub.SubscriptionHub
	tailManager              *TailManager
	matchCollector           search.MatchCollector
	searchPeer               *dmesh.SearchPeer
	dmeshClient              dmeshClient.SearchClient
	shutdownDelay            time.Duration
	headDelayTolerance       uint64
}

func New(dmeshClient dmeshClient.SearchClient, searchPeer *dmesh.SearchPeer, headDelayTolerance uint64, shutdownDelay time.Duration) *LiveBackend {
	matchCollector := search.GetMatchCollector
	if matchCollector == nil {
		panic(fmt.Errorf("no match collector set, should not happen, you should define a collector"))
	}

	live := &LiveBackend{
		Shutter:                  shutter.New(),
		nextTierBackendsBlockNum: &atomic.Uint64{},
		matchCollector:           matchCollector,
		dmeshClient:              dmeshClient,
		searchPeer:               searchPeer, // local reversible peer
		headDelayTolerance:       headDelayTolerance,
		shutdownDelay:            shutdownDelay,
	}

	live.OnTerminating(func(e error) {
		zlog.Info("shutting down search live", zap.Error(e))
		live.stop()
	})
	return live
}

func (b *LiveBackend) startServer(listenAddr string) {
	// gRPC
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Shutter.Shutdown(fmt.Errorf("failed listening grpc %q: %w", listenAddr, err))
		return
	}

	s := dgrpc.NewServer(dgrpc.WithLogger(zlog))
	pb.RegisterBackendServer(s, b)
	pbhead.RegisterStreamingHeadInfoServer(s, b)
	pbhead.RegisterHeadInfoServer(s, b)
	pbhealth.RegisterHealthServer(s, b)

	zlog.Info("setting peer to ready and publishing")
	b.searchPeer.Locked(func() {
		b.searchPeer.Ready = true
	})
	err = b.dmeshClient.PublishNow(b.searchPeer)
	if err != nil {
		b.Shutter.Shutdown(fmt.Errorf("unable to publish live search peer: %w", err))
		return
	}

	go func() {
		zlog.Info("listening & serving gRPC content", zap.String("grpc_listen_addr", listenAddr))
		if err := s.Serve(lis); err != nil {
			b.Shutter.Shutdown(fmt.Errorf("error on gs.Serve: %w", err))
			return
		}
	}()
}

func (b *LiveBackend) Launch(grpcListenAddr string) {

	b.startServer(grpcListenAddr)

	select {
	case <-b.Terminating():
		if err := b.Err(); err != nil {
			zlog.Error("search live terminated with error", zap.Error(err))
		} else {
			zlog.Info("search live terminated")
		}
	}

}

func (b *LiveBackend) stop() {
	zlog.Info("setting search peer ")
	b.searchPeer.Locked(func() {
		b.searchPeer.Ready = false
	})
	b.dmeshClient.PublishNow(b.searchPeer)

	zlog.Info("shutting down live search, setting ready flag to false", zap.Duration("shutdown_delay", b.shutdownDelay))
	time.Sleep(b.shutdownDelay)
}

// Backend.StreamMatches gRPC implementation
func (b *LiveBackend) StreamMatches(req *pb.BackendRequest, stream pb.Backend_StreamMatchesServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	zlogger := logging.Logger(ctx, zlog)

	zlogger.Debug("starting live backend query", zap.Reflect("request", req))
	bquery, err := search.NewParsedQuery(ctx, req.Query)
	if err != nil {
		if err == context.Canceled {
			return derr.Status(codes.Canceled, "context canceled")
		}
		return err
	}

	// Incoming request's LowBlockNum and HighBlockNum are absolute,

	// DMESH: TODO: Handle the flag to indicate we should actually stop at HEAD instead of going
	// into eternity.
	metrics.ActiveQueryCount.Inc()
	defer metrics.ActiveQueryCount.Dec()

	trailer := metadata.New(nil)
	defer stream.SetTrailer(trailer) // set trailer before canceling context (thus, after in code)

	// set the trailer as a default -1 in case we error out
	trailer.Set("last-block-read", fmt.Sprint("-1"))

	liveQuery := b.newLiveQuery(ctx, req, bquery)

	lib := b.tailManager.CurrentLIB()
	if err := liveQuery.run(lib, b.headDelayTolerance, stream.Send); err != nil {
		if err == context.Canceled {
			return derr.Status(codes.Canceled, "context canceled")
		}
		return err
	}

	trailer.Set("last-block-read", fmt.Sprintf("%d", liveQuery.LastBlockRead))

	return nil
}

func (b *LiveBackend) GetHeadInfo(ctx context.Context, r *pbhead.HeadInfoRequest) (*pbhead.HeadInfoResponse, error) {
	firstNum, firstID, _, _, headNum, headID := b.searchPeer.HeadBlockPointers()
	resp := &pbhead.HeadInfoResponse{
		LibNum:  firstNum,
		LibID:   firstID,
		HeadNum: headNum,
		HeadID:  headID,
	}
	return resp, nil
}

func (b *LiveBackend) StreamHeadInfo(r *pbhead.HeadInfoRequest, stream pbhead.StreamingHeadInfo_StreamHeadInfoServer) error {
	for {
		resp, _ := b.GetHeadInfo(stream.Context(), r)

		if err := stream.Send(resp); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
	}
}
