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
	"os"
	"strings"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/dfuse-io/dstore"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	livebackend "github.com/dfuse-io/search/live"
	"github.com/dfuse-io/search/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	ServiceVersion           string        // dmesh service version (v1)
	TierLevel                uint32        // level of the search tier
	GRPCListenAddr           string        // Address to listen for incoming gRPC requests
	PublishInterval          time.Duration // longest duration a dmesh peer will not publish
	BlockmetaAddr            string        // grpc address to blockmeta to decide if the chain is up-to-date
	BlocksStoreURL           string        // Path to read blocks archives
	BlockstreamAddr          string        // gRPC URL to reach a stream of blocks
	HeadDelayTolerance       uint64        // Number of blocks above a backend's head we allow a request query to be served (Live & Router)
	StartBlockDriftTolerance uint64        // Number of blocks behind LIB that the start block is allowed to be
	ShutdownDelay            time.Duration // On shutdown, time to wait before actually leaving, to try and drain connections
	LiveIndexesPath          string        // /tmp/live/indexes", "Location for live indexes (ideally a ramdisk)
	TruncationThreshold      int           //number of available dmesh peers that should serve irreversible blocks before we truncate them from this backend's memory
	RealtimeTolerance        time.Duration // longest delay to consider this service as real-time(ready) on initialization
	HubChannelSize           int           // the number of blocks that can be sent in the hub channel before is reaches capacity
	PreProcConcurrentThreads int
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
	BlockMapper search.BlockMapper
	Dmesh       dmeshClient.SearchClient
	Tracker     *bstream.Tracker // Prepared with StartBlockResolvers.
}

var LiveAppStartAborted = fmt.Errorf("getting start block aborted by live application")

type App struct {
	*shutter.Shutter
	config         *Config
	modules        *Modules
	readinessProbe pbhealth.HealthClient
}

func New(config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
	}
}
func (a *App) Run() error {
	appCtx, cancel := context.WithCancel(context.Background())
	a.Shutter.OnTerminating(func(_ error) {
		cancel()
	})

	zlog.Info("running live app ", zap.Reflect("config", a.config))

	metrics.Register(metrics.LiveMetricSet)

	if err := search.ValidateRegistry(); err != nil {
		return err
	}

	zlog.Info("starting dmesh")
	err := a.modules.Dmesh.Start(context.Background(), []string{
		"/" + a.config.ServiceVersion + "/search",
	})
	if err != nil {
		return fmt.Errorf("unable to start dmesh client: %w", err)
	}

	zlog.Info("clearing working directory", zap.Reflect("working_directory", a.config.LiveIndexesPath))
	err = os.RemoveAll(a.config.LiveIndexesPath)
	if err != nil {
		return fmt.Errorf("unable to clear working directory: %w", err)
	}

	blocksStore, err := dstore.NewDBinStore(a.config.BlocksStoreURL)
	if err != nil {
		return fmt.Errorf("failed setting up blocks store: %w", err)
	}

	zlog.Info("creating search peer")
	searchPeer := dmesh.NewSearchHeadPeer(a.config.ServiceVersion, a.config.GRPCListenAddr, 1, a.config.TierLevel, a.config.PublishInterval)

	zlog.Info("publishing search archive peer", zap.String("peer_host", searchPeer.GenericPeer.Host))
	err = a.modules.Dmesh.PublishNow(searchPeer)
	if err != nil {
		return fmt.Errorf("publishing peer to dmesh: %w", err)
	}

	lb := livebackend.New(a.modules.Dmesh, searchPeer, a.config.HeadDelayTolerance, a.config.ShutdownDelay)

	zlog.Info("setting up blockmeta")
	blockMetaClient, err := pbblockmeta.NewClient(a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("new block meta client: %w", err)
	}

	tracker := a.modules.Tracker.Clone()
	tracker.SetNearBlocksCount(int64(a.config.StartBlockDriftTolerance))
	tracker.AddGetter(search.DmeshArchiveLIBTarget, search.DmeshHighestArchiveBlockRefGetter(a.modules.Dmesh.Peers, 1))
	//tracker.AddGetter(bstream.NetworkLIBTarget, bstream.HighestBlockRefGetter(bstream.StreamLIBBlockRefGetter(a.config.BlockstreamAddr), bstream.NetworkLIBBlockRefGetter(a.config.BlockmetaAddr)))
	tracker.AddGetter(bstream.NetworkLIBTarget, bstream.NetworkLIBBlockRefGetter(a.config.BlockmetaAddr))

	zlog.Info("blockmeta setup getting start block")
	startLIB, err := a.getStartLIB(appCtx, tracker, blockMetaClient)
	if err != nil {
		if err == LiveAppStartAborted {
			return nil
		}
		return err
	}
	//FIXME the tail manager should have two modes of working: 1) based on archive and 2) based on a buffer length, in case the archive has never met its lower bound
	if startLIB == nil {
		zlog.Info("live got a nil start block")
		return nil
	}

	zlog.Info("search live resolved start block",
		zap.String("start_lib_id", startLIB.ID()),
		zap.Uint64("start_lib_num", startLIB.Num()),
	)

	zlog.Info("setting up subscription hub", zap.Uint64("start_block", startLIB.Num()))
	err = lb.SetupSubscriptionHub(
		startLIB,
		a.modules.BlockFilter,
		a.modules.BlockMapper,
		blocksStore,
		a.config.BlockstreamAddr,
		a.config.LiveIndexesPath,
		a.config.RealtimeTolerance,
		a.config.TruncationThreshold,
		a.config.PreProcConcurrentThreads,
		a.config.HubChannelSize,
	)
	if err != nil {
		return fmt.Errorf("setting up subscription hub: %w", err)
	}

	a.OnTerminating(lb.Shutdown)
	lb.OnTerminated(a.Shutdown)

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	zlog.Info("launching live search")
	go func() {
		lb.WaitHubReady(appCtx)
		if a.IsTerminating() {
			// No need to continue if we are terminating
			return
		}

		lb.Launch(a.config.GRPCListenAddr)
	}()

	return nil
}

func (a *App) getStartLIB(ctx context.Context, tracker *bstream.Tracker, blockIDClient *pbblockmeta.Client) (startBlockRef bstream.BlockRef, err error) {
	sleepTime := time.Duration(0)
	for {
		if a.IsTerminating() {
			zlog.Info("leaving getStartLIB because app is terminating")
			err = LiveAppStartAborted
			return
		}
		time.Sleep(sleepTime)
		sleepTime = time.Second * 2

		archiveLIB, _, isNear, err := tracker.IsNearWithResults(ctx, search.DmeshArchiveLIBTarget, bstream.NetworkLIBTarget)
		if err != nil {
			level := zap.WarnLevel

			// Sucks but the errors it not a multi-error, so it's not wrapped and hence, we cannot walk it "nicely"
			if strings.HasSuffix(err.Error(), bstream.ErrTrackerBlockNotFound.Error()) {
				level = zap.InfoLevel
			}

			zlog.Check(level, "failed to get is near with results").Write(zap.Error(err))
			continue
		}
		if !isNear {
			zlog.Info("not near, will retry", zap.Reflect("archive_lib", archiveLIB))
			time.Sleep(1 * time.Second)
			continue
		}

		if archiveLIB == nil {
			zlog.Info("stream at the beginning of chain, archive not ready, using network lib")
			idResponse, err := blockIDClient.BlockNumToID(ctx, bstream.GetProtocolFirstStreamableBlock)
			if err != nil {
				level := zap.WarnLevel
				if status.Code(err) == codes.Unavailable {
					level = zap.InfoLevel
				}

				zlog.Check(level, "failed to get block id for, retrying...").Write(zap.Uint64("first_streamable_block", bstream.GetProtocolFirstStreamableBlock), zap.Error(err))
				continue
			}
			return bstream.NewBlockRef(idResponse.Id, bstream.GetProtocolFirstStreamableBlock), nil
		}
		return archiveLIB, nil
	}
}

func (a *App) IsReady() bool {
	if a.readinessProbe == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := a.readinessProbe.Check(ctx, &pbhealth.HealthCheckRequest{})
	if err != nil {
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}
