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

package archive_bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"net"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/dfuse-io/logging"
	pbhead "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	pbsearch "github.com/dfuse-io/pbgo/dfuse/search/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	pmetrics "github.com/dfuse-io/search/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// Search is the top-level object, embodying the rest.
type ArchiveBigQueryBackend struct {
	*shutter.Shutter

	SearchPeer      *dmesh.SearchPeer
	dmeshClient     dmeshClient.Client
	grpcListenAddr  string
	bigQueryDSN     string
	shuttingDown    *atomic.Bool
	shutdownDelay   time.Duration

	bigqueryClient *bigquery.Client
}

func NewBackend(
	dmeshClient dmeshClient.Client,
	searchPeer *dmesh.SearchPeer,
	grpcListenAddr string,
	bigQueryDSN string,
	shutdownDelay time.Duration,
) *ArchiveBigQueryBackend {

	archive := &ArchiveBigQueryBackend{
		Shutter:        shutter.New(),
		dmeshClient:    dmeshClient,
		SearchPeer:     searchPeer,
		grpcListenAddr: grpcListenAddr,
		bigQueryDSN: bigQueryDSN,
		shuttingDown:   atomic.NewBool(false),
		shutdownDelay:  shutdownDelay,
	}

	return archive
}


func (b *ArchiveBigQueryBackend) startServer() {
	ctx := context.Background()
	c, err := bigquery.NewClient(ctx, "")
	if err != nil {
		// TODO: Handle error.
	}
	b.bigqueryClient = c

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

func (b *ArchiveBigQueryBackend) GetHeadInfo(ctx context.Context, r *pbhead.HeadInfoRequest) (*pbhead.HeadInfoResponse, error) {
	resp := &pbhead.HeadInfoResponse{
		LibNum: 100000,
	}
	return resp, nil
}

// headinfo.StreamingHeadInfo gRPC implementation
func (b *ArchiveBigQueryBackend) StreamHeadInfo(r *pbhead.HeadInfoRequest, stream pbhead.StreamingHeadInfo_StreamHeadInfoServer) error {
	//for {
	//	resp, _ := b.GetHeadInfo(stream.Context(), r)
	//
	//	if err := stream.Send(resp); err != nil {
	//		return err
	//	}
	//	time.Sleep(500 * time.Millisecond)
	//}
	return nil
}

// Archive.StreamMatches gRPC implementation
func (b *ArchiveBigQueryBackend) StreamMatches(req *pbsearch.BackendRequest, stream pbsearch.Backend_StreamMatchesServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	if req.WithReversible {
		return fmt.Errorf("archive-bigquery backend does not support WithReversible == true")
	}
	if req.StopAtVirtualHead {
		return fmt.Errorf("archive-bigquery backend does not support StopAtVirtualHead == true")
	}
	if req.LiveMarkerInterval != 0 {
		return fmt.Errorf("archive-bigquery backend does not support LiveMarkerInterval != 0")
	}
	if req.NavigateFromBlockID != "" {
		return fmt.Errorf("archive-bigquery backend does not support NavigateFromBlockID != ''")
	}
	if req.NavigateFromBlockNum != 0 {
		return fmt.Errorf("archive-bigquery backend does not support NavigateFromBlockNum != 0")
	}

	zlogger := logging.Logger(ctx, zlog)
	zlogger.Info("starting streaming search query processing")

	bquery, err := search.NewParsedQuery(req.Query)
	if err != nil {
		return err // status.New(codes.InvalidArgument, err.Error())
	}

	metrics := search.NewQueryMetrics(zlogger, req.Descending, bquery.Raw, 0, req.LowBlockNum, req.HighBlockNum)
	defer metrics.Finalize()

	pmetrics.ActiveQueryCount.Inc()
	defer pmetrics.ActiveQueryCount.Dec()

	trailer := metadata.New(nil)
	defer stream.SetTrailer(trailer)

	//// set the trailer as a default -1 in case we error out
	trailer.Set("last-block-read", fmt.Sprint("-1"))


	//TODO: bounds check...
	//first, _, irr, _, _, _ := b.SearchPeer.HeadBlockPointers()
	//if err := archiveQuery.checkBoundaries(first, irr); err != nil {
	//	return err
	//}

	q := b.bigqueryClient.Query(`
    SELECT year, SUM(number) as num
    FROM ` + "`bigquery-public-data.usa_names.usa_1910_2013`" + `
    WHERE name = "William"
    GROUP BY year
    ORDER BY year
	`)
	it, err := q.Read(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			//TODO fix
			trailer.Set("last-block-read", fmt.Sprintf("%d", 1000000))
			return nil
		}
		if err != nil {
			if ctx.Err() == context.Canceled {
				// error is most likely not from us, but happened upstream
				return nil
			}

			zlogger.Error("archive query received error from channel", zap.Error(err))
			return err
		}

		metrics.TransactionSeenCount++

		response, err := archiveSearchMatchToProto(convertBigQueryResultsToSearchMatch(values))
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

func convertBigQueryResultsToSearchMatch(values []bigquery.Value) search.SearchMatch {
	return nil
}

func (b *ArchiveBigQueryBackend) Launch() {
	b.OnTerminating(func(e error) {
		zlog.Info("shutting down search archive", zap.Error(e))
		b.stop()
	})

	b.startServer()

	select {
	case <-b.Terminating():
		zlog.Info("archive-bigquery backend terminated")
		if err := b.Err(); err != nil {
			err = fmt.Errorf("archive backend terminated with error: %s", err)
		}
	}
}

func (b *ArchiveBigQueryBackend) stop() {
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
}
