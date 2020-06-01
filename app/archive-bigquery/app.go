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
	"bufio"
	"context"
	"fmt"
	"github.com/dfuse-io/search/archive"
	"os"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	archiveBigQuery "github.com/dfuse-io/search/archive-bigquery"
	"github.com/dfuse-io/search/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	// dmesh configuration
	ServiceVersion          string        // dmesh service version (v1)
	TierLevel               uint32        // level of the search tier
	GRPCListenAddr          string        // Address to listen for incoming gRPC requests
	PublishInterval         time.Duration // longest duration a dmesh peer will not publish
	BigQueryDSN             string        // DSN of the BigQuery Dataset
	ShutdownDelay           time.Duration //On shutdown, time to wait before actually leaving, to try and drain connections
}

type Modules struct {
	Dmesh dmeshClient.SearchClient
}

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
	zlog.Info("running archive-bigquery app ", zap.Reflect("config", a.config))

	metrics.Register(metrics.ArchiveBigQueryMetricsSet)

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

	zlog.Info("creating search peer")
	searchPeer := dmesh.NewSearchArchivePeer(a.config.ServiceVersion, a.config.GRPCListenAddr, false, true, 0, a.config.TierLevel, a.config.PublishInterval)

	zlog.Info("publishing search archive peer", zap.String("peer_host", searchPeer.GenericPeer.Host))
	err = a.modules.Dmesh.PublishNow(searchPeer)
	if err != nil {
		return fmt.Errorf("publishing peer to dmesh: %w", err)
	}

	lastIrrBlockNum := uint64(0)
	lastIrrBlockID := "xyz"
	resolvedStartBlockNum := uint64(0)

	zlog.Info("base irreversible block to start with", zap.Uint64("lib_num", lastIrrBlockNum), zap.String("lib_id", lastIrrBlockID), zap.Uint64("start_block", resolvedStartBlockNum))
	metrics.TailBlockNumber.SetUint64(resolvedStartBlockNum)
	searchPeer.Locked(func() {
		searchPeer.IrrBlock = lastIrrBlockNum
		searchPeer.IrrBlockID = lastIrrBlockID
		searchPeer.HeadBlock = lastIrrBlockNum
		searchPeer.HeadBlockID = lastIrrBlockID
		searchPeer.TailBlock = resolvedStartBlockNum
	})
	err = a.modules.Dmesh.PublishNow(searchPeer)
	if err != nil {
		return fmt.Errorf("publishing peer to dmesh: %w", err)
	}

	zlog.Info("setting up archive-bigquery backend")
	archiveBigQueryBackend := archiveBigQuery.NewBackend(a.modules.Dmesh, searchPeer, a.config.GRPCListenAddr, a.config.BigQueryDSN, a.config.ShutdownDelay)

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	a.OnTerminating(func(e error) {
		zlog.Info("archive-bigquery application is terminating, shutting down archive backend")
		archiveBigQueryBackend.Shutdown(e)
		zlog.Info("archive-bigquery backend shutdown complete")
	})
	archiveBigQueryBackend.OnTerminated(func(e error) {
		zlog.Info("archive-bigquery backend terminated , shutting down archive application")
		a.Shutdown(e)
		zlog.Info("archive-bigquery application shutdown complete")
	})

	zlog.Info("launching archive-bigquery backend")
	go archiveBigQueryBackend.Launch()

	return nil
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

func resolveStartBlock(startBlock int64, shardSize, irrBlockNum uint64) (uint64, error) {
	zlog.Info("resolving start block", zap.Int64("start_block", startBlock), zap.Uint64("shard_size", shardSize), zap.Uint64("irr_block_num", irrBlockNum))
	if startBlock >= 0 {
		if startBlock%int64(shardSize) != 0 {
			return 0, fmt.Errorf("start block %d misaligned with shard size %d", startBlock, shardSize)
		} else {
			return uint64(startBlock), nil
		}
	}
	absoluteStartBlock := (int64(irrBlockNum) + startBlock)
	absoluteStartBlock = absoluteStartBlock - (absoluteStartBlock % int64(shardSize))
	if absoluteStartBlock < 0 {
		return 0, fmt.Errorf("relative start block %d  is to large, cannot resolve to a negative start block %d", startBlock, absoluteStartBlock)
	}
	return uint64(absoluteStartBlock), nil
}

func getSearchHighestIrr(peers []*dmesh.SearchPeer) (irrBlock uint64) {
	zlog.Info("getting highest irr block num", zap.Int("peer_count", len(peers)))
	for _, peer := range peers {
		zlog.Info("getting highest irr block num", zap.String("peer", peer.Host), zap.Uint64("irr_block_num", peer.IrrBlock))
		if peer.IrrBlock > irrBlock {
			irrBlock = peer.IrrBlock
		}
	}
	return irrBlock
}

func warmupSearch(filepath string, firstIndexedBlock, lastIndexedBlock uint64, engine *archive.ArchiveBackend) error {
	zlog.Info("warming up", zap.Uint64("first_indexed_block", firstIndexedBlock), zap.Uint64("last_indexed_block", lastIndexedBlock))
	now := time.Now()
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("cannot open search warmup queries: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		err := engine.WarmupWithQuery(scanner.Text(), firstIndexedBlock, lastIndexedBlock)
		if err != nil {
			return fmt.Errorf("cannot warmup: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning error: %w", err)
	}

	zlog.Info("warmup completed", zap.Duration("duration", time.Since(now)))
	return nil
}

func getBlockCount(startBlock int64) (uint64, error) {
	if startBlock >= 0 {
		return 0, fmt.Errorf("start block %d must be a relative value (-) to yield a block count", startBlock)
	}
	return uint64(-1 * startBlock), nil
}
