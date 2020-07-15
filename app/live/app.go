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
	"time"

	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"

	"github.com/dfuse-io/search/metrics"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/dfuse-io/dstore"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/live"
	livebackend "github.com/dfuse-io/search/live"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
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
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
	BlockMapper search.BlockMapper
	Dmesh       dmeshClient.SearchClient
	Tracker     *bstream.Tracker
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

	conn, err := dgrpc.NewInternalClient(a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("getting blockmeta headinfo client: %w", err)
	}
	headinfoCli := pbheadinfo.NewHeadInfoClient(conn)
	blockMetaClient, err := pbblockmeta.NewClient(a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("new block meta client: %w", err)
	}

	zlog.Info("blockmeta setup getting start block")
	startLIB, err := a.getStartLIB(context.Background(), a.modules.Dmesh, headinfoCli, blockMetaClient)
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
		lb.WaitHubReady()
		lb.Launch(a.config.GRPCListenAddr)
	}()

	return nil
}

func startBlockFromDmesh(dmesh dmeshClient.SearchClient) bstream.BlockRef {
	zlog.Info("start block from dmesh", zap.Int("peer count", len(dmesh.Peers())))
	libBlock := live.GetMeshLIB(dmesh.Peers, 1)
	if libBlock != nil {
		zlog.Info("lib from dmesh", zap.Uint64("block_num", libBlock.Num()), zap.String("block_id", libBlock.ID()))
		if libBlock.Num() < bstream.GetProtocolFirstStreamableBlock {
			return bstream.NewBlockRef("", bstream.GetProtocolFirstStreamableBlock)
		}
		return libBlock
	}

	zlog.Info("lib from dmesh was nil")

	return nil
}
func libFromHeadInfo(headinfoCli pbheadinfo.HeadInfoClient, source pbheadinfo.HeadInfoRequest_Source) bstream.BlockRef {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	streamInfo, err := headinfoCli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{
		Source: source,
	})
	if err != nil {
		zlog.Debug("cannot get lib from headinfo", zap.Error(err))
		return nil
	}

	return bstream.NewBlockRef(streamInfo.LibID, streamInfo.LibNum)
}

func tweakStartBlock(blk bstream.BlockRef) bstream.BlockRef {
	if blk.Num() < 2 {
		return bstream.NewBlockRef("", 2)
	}
	return blk
}

func (a *App) getStartLIB(ctx context.Context, dmesh dmeshClient.SearchClient, headinfoCli pbheadinfo.HeadInfoClient, blockIDClient *pbblockmeta.Client) (startBlockRef bstream.BlockRef, err error) {
	sleepTime := time.Duration(0)
	for {
		if a.IsTerminating() {
			zlog.Info("leaving getStartLIB because app is terminating")
			err = LiveAppStartAborted
			return
		}
		time.Sleep(sleepTime)
		sleepTime = time.Second * 2

		// StreamHeadBlockRefGetter(headinfoCli)
		archiveLIB, networkLIB, isNear := a.modules.Tracker.IsNearWithResults(search.DmeshArchiveLIBTarget, bstream.NetworkLIBTarget)
		if !isNear {
			time.Sleep(1 * time.Minute)
			continue
		}

		if archiveLIB == nil {
			zlog.Info("stream at the beginning of chain, archive not ready, using network lib")
			idResponse, err := blockIDClient.BlockNumToID(ctx, bstream.GetProtocolFirstStreamableBlock)
			if err != nil {
				zlog.Warn("failed to get block id for, retrying...", zap.Uint64("first_streamable_block", bstream.GetProtocolFirstStreamableBlock), zap.Error(err))
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
		zlog.Info("readiness probe error", zap.Error(err))
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}
