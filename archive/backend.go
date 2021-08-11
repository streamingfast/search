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

package archive

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/streamingfast/dgrpc"
	"github.com/dfuse-io/logging"
	pbhead "github.com/streamingfast/pbgo/dfuse/headinfo/v1"
	pbsearch "github.com/streamingfast/pbgo/dfuse/search/v1"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"github.com/gorilla/mux"
	"github.com/streamingfast/dmesh"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search"
	pmetrics "github.com/streamingfast/search/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// Search is the top-level object, embodying the rest.
type ArchiveBackend struct {
	*shutter.Shutter

	Pool            *IndexPool
	SearchPeer      *dmesh.SearchPeer
	dmeshClient     dmeshClient.Client
	grpcListenAddr  string
	httpListenAddr  string
	matchCollector  search.MatchCollector
	httpServer      *http.Server
	MaxQueryThreads int
	shuttingDown    *atomic.Bool
	shutdownDelay   time.Duration
}

func NewBackend(
	pool *IndexPool,
	dmeshClient dmeshClient.Client,
	searchPeer *dmesh.SearchPeer,
	grpcListenAddr string,
	httpListenAddr string,
	shutdownDelay time.Duration,
) *ArchiveBackend {

	matchCollector := search.GetMatchCollector
	if matchCollector == nil {
		panic(fmt.Errorf("no match collector set, should not happen, you should define a collector"))
	}

	archive := &ArchiveBackend{
		Shutter:        shutter.New(),
		Pool:           pool,
		dmeshClient:    dmeshClient,
		SearchPeer:     searchPeer,
		grpcListenAddr: grpcListenAddr,
		httpListenAddr: httpListenAddr,
		matchCollector: matchCollector,
		shuttingDown:   atomic.NewBool(false),
		shutdownDelay:  shutdownDelay,
	}

	return archive
}

func (b *ArchiveBackend) SetMaxQueryThreads(threads int) {
	b.MaxQueryThreads = threads
}

// FIXME: are we *really* servicing some things through REST ?!  That
// `indexed_fields` should be served via gRPC.. all those middlewares,
// gracking, logging, etc.. wuuta
func (b *ArchiveBackend) startServer() {
	router := mux.NewRouter()

	metricsRouter := router.PathPrefix("/").Subrouter()

	// Metrics & health endpoints
	metricsRouter.HandleFunc("/healthz", b.healthzHandler())

	// HTTP
	b.httpServer = &http.Server{Addr: b.httpListenAddr, Handler: router}
	go func() {
		zlog.Info("listening & serving HTTP content", zap.String("http_listen_addr", b.httpListenAddr))
		if err := b.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			b.Shutter.Shutdown(fmt.Errorf("failed listening http %q: %w", b.httpListenAddr, err))
			return
		}
	}()

	// gRPC
	lis, err := net.Listen("tcp", b.grpcListenAddr)
	if err != nil {
		b.Shutter.Shutdown(fmt.Errorf("failed listening grpc %q: %w", b.grpcListenAddr, err))
		return
	}

	s := dgrpc.NewServer(dgrpc.WithLogger(zlog))

	pbsearch.RegisterBackendServer(s, b)
	pbhead.RegisterStreamingHeadInfoServer(s, b)
	pbhealth.RegisterHealthServer(s, b)

	go func() {
		zlog.Info("listening & serving gRPC content", zap.String("grpc_listen_addr", b.grpcListenAddr))
		if err := s.Serve(lis); err != nil {
			b.Shutter.Shutdown(fmt.Errorf("error on gs.Serve: %w", err))
			return
		}
	}()
}

func (b *ArchiveBackend) GetHeadInfo(ctx context.Context, r *pbhead.HeadInfoRequest) (*pbhead.HeadInfoResponse, error) {
	resp := &pbhead.HeadInfoResponse{
		LibNum: b.Pool.LastReadOnlyIndexedBlock(),
	}
	return resp, nil
}

// headinfo.StreamingHeadInfo gRPC implementation
func (b *ArchiveBackend) StreamHeadInfo(r *pbhead.HeadInfoRequest, stream pbhead.StreamingHeadInfo_StreamHeadInfoServer) error {
	for {
		resp, _ := b.GetHeadInfo(stream.Context(), r)

		if err := stream.Send(resp); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (b *ArchiveBackend) WarmupWithQuery(query string, low, high uint64) error {
	ctx := context.Background()
	bquery, err := search.NewParsedQuery(ctx, query)
	if err != nil {
		return err
	}

	return b.WarmUpArchive(ctx, low, high, bquery)
}

// Archive.StreamMatches gRPC implementation
func (b *ArchiveBackend) StreamMatches(req *pbsearch.BackendRequest, stream pbsearch.Backend_StreamMatchesServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	if req.WithReversible {
		return fmt.Errorf("archive backend does not support WithReversible == true")
	}
	if req.StopAtVirtualHead {
		return fmt.Errorf("archive backend does not support StopAtVirtualHead == true")
	}
	if req.LiveMarkerInterval != 0 {
		return fmt.Errorf("archive backend does not support LiveMarkerInterval != 0")
	}
	if req.NavigateFromBlockID != "" {
		return fmt.Errorf("archive backend does not support NavigateFromBlockID != ''")
	}
	if req.NavigateFromBlockNum != 0 {
		return fmt.Errorf("archive backend does not support NavigateFromBlockNum != 0")
	}

	zlogger := logging.Logger(ctx, zlog)
	zlogger.Info("starting streaming search query processing")

	bquery, err := search.NewParsedQuery(ctx, req.Query)
	if err != nil {
		return err // status.New(codes.InvalidArgument, err.Error())
	}

	metrics := search.NewQueryMetrics(zlogger, req.Descending, bquery.Raw, b.Pool.ShardSize, req.LowBlockNum, req.HighBlockNum)
	defer metrics.Finalize()

	pmetrics.ActiveQueryCount.Inc()
	defer pmetrics.ActiveQueryCount.Dec()

	trailer := metadata.New(nil)
	defer stream.SetTrailer(trailer)

	// set the trailer as a default -1 in case we error out
	trailer.Set("last-block-read", fmt.Sprint("-1"))

	archiveQuery := b.newArchiveQuery(ctx, req.Descending, req.LowBlockNum, req.HighBlockNum, bquery, metrics)

	first, _, irr, _, _, _ := b.SearchPeer.HeadBlockPointers()
	if err := archiveQuery.checkBoundaries(first, irr); err != nil {
		return err
	}

	go archiveQuery.run()

	for {
		select {
		case err := <-archiveQuery.Errors:
			if err != nil {
				if ctx.Err() == context.Canceled {
					// error is most likely not from us, but happened upstream
					return nil
				}

				zlogger.Error("archive query received error from channel", zap.Error(err))
				return err
			}
			return nil

		case match, ok := <-archiveQuery.Results:
			if !ok {

				if !archiveQuery.ProcessedShard {
					return fmt.Errorf("search backend did not process any shard, potential block range routing issue advertising ranges we don't serve")
				} else {
					trailer.Set("last-block-read", fmt.Sprintf("%d", archiveQuery.LastBlockRead.Load()))
				}

				return nil
			}

			metrics.TransactionSeenCount++

			response, err := archiveSearchMatchToProto(match)
			if err != nil {
				return fmt.Errorf("unable to obtain search match proto: %s", err)
			}

			metrics.MarkFirstResult()
			if err := stream.Send(response); err != nil {
				// Upstream wants us to stop, this is `io.EOF` ?
				zlogger.Info("we've had a failure sending this upstream, interrupt all this search now")
				return err
			}
		}
	}
}

func (b *ArchiveBackend) Launch() {
	b.OnTerminating(func(e error) {
		zlog.Info("shutting down search archive", zap.Error(e))
		b.stop()
	})

	b.startServer()

	select {
	case <-b.Terminating():
		zlog.Info("archive backend terminated")
		if err := b.Err(); err != nil {
			err = fmt.Errorf("archive backend terminated with error: %s", err)
		}
	}
}

func (b *ArchiveBackend) shutdownHTTPServer() error { /* gracefully */
	if b.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return b.httpServer.Shutdown(ctx)
	}
	return nil
}

func (b *ArchiveBackend) stop() {
	zlog.Info("cleaning up archive backend", zap.Duration("shutdown_delay", b.shutdownDelay))
	// allow kube service the time to finish in-flight request before the service stops
	// routing traffic
	// We are probably on batch mode where no search peer exists, so don't publish it
	if b.SearchPeer != nil {
		b.SearchPeer.Locked(func() {
			b.SearchPeer.Ready = false
		})
		err := b.dmeshClient.PublishNow(b.SearchPeer)
		if err != nil {
			zlog.Error("could not set search peer to not ready", zap.Error(err))
		}
	}

	b.shuttingDown.Store(true)
	time.Sleep(b.shutdownDelay)

	// Graceful shutdown of HTTP server, drain connections, before closing indexes.
	zlog.Info("gracefully shutting down http server, draining connections")
	err := b.shutdownHTTPServer()
	zlog.Info("shutdown http server", zap.Error(err))

	zlog.Info("closing indexes cleanly")
	err = b.Pool.CloseIndexes()
	if err != nil {
		zlog.Error("error closing indexes", zap.Error(err))
	}
}
