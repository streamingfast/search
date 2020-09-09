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

package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/indexer"
	"github.com/dfuse-io/search/metrics"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	HTTPListenAddr        string // path for http healthcheck
	GRPCListenAddr        string // path for gRPC healthcheck
	IndicesStoreURL       string // Path to upload the wirtten index shards
	BlocksStoreURL        string // Path to read blocks archives
	BlockstreamAddr       string // gRPC URL to reach a stream of blocks
	WritablePath          string // Writable base path for storing index files
	ShardSize             uint64 // Number of blocks to store in a given Bleve index
	StartBlock            int64  // Start indexing from block num
	StopBlock             uint64 // Stop indexing at block num
	IsVerbose             bool   // verbose logging
	EnableBatchMode       bool   // Enabled the indexer in batch mode with a start & stop block
	EnableUpload          bool   // Upload merged indexes to the --indexes-store
	DeleteAfterUpload     bool   // Delete local indexes after uploading them
	EnableIndexTruncation bool   // Enable index truncation, requires a relative --start-block (negative number)
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
	BlockMapper search.BlockMapper
	Tracker     *bstream.Tracker
}

var IndexerAppStartAborted = fmt.Errorf("getting irr block aborted by indexer application")

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

// resolveStartBlock will attempt to
//  1) Get your desired target start block: the value at which you start processing blocks
//	2) Get the filesource start block and IRR: the value at which you will start your source
func (a *App) resolveStartBlock(ctx context.Context, dexer *indexer.Indexer) (targetStartBlock uint64, filesourceStartBlock uint64, previousIrreversibleID string, err error) {
	if a.config.EnableBatchMode {
		if a.config.StartBlock < 0 {
			return 0, 0, "", fmt.Errorf("invalid negative start block in batch mode")
		}
		targetStartBlock = uint64(a.config.StartBlock)
	} else {
		targetStartBlock, err = a.modules.Tracker.GetRelativeBlock(ctx, a.config.StartBlock, bstream.NetworkLIBTarget)
		if err != nil {
			return
		}
		targetStartBlock = dexer.NextUnindexedBlockPast(targetStartBlock) // skip already processed indexes
	}

	if targetStartBlock < bstream.GetProtocolFirstStreamableBlock {
		targetStartBlock = bstream.GetProtocolFirstStreamableBlock
	}

	filesourceStartBlock, previousIrreversibleID, err = a.modules.Tracker.ResolveStartBlock(ctx, targetStartBlock)
	if err != nil {
		err = fmt.Errorf("tacker: failed to resolve start block: %w", err)
	}
	return
}

func (a *App) Run() error {
	zlog.Info("running indexer app ", zap.Reflect("config", a.config))

	metrics.Register(metrics.IndexerMetricSet)

	if err := search.ValidateRegistry(); err != nil {
		return err
	}

	indexesStore, err := dstore.NewStore(a.config.IndicesStoreURL, "", "zstd", true)
	if err != nil {
		return fmt.Errorf("failed setting up indexes store: %w", err)
	}

	blocksStore, err := dstore.NewDBinStore(a.config.BlocksStoreURL)
	if err != nil {
		return fmt.Errorf("failed setting up blocks store: %w", err)
	}

	dexer := indexer.NewIndexer(
		indexesStore,
		blocksStore,
		a.config.BlockstreamAddr,
		a.modules.BlockFilter,
		a.modules.BlockMapper,
		a.config.WritablePath,
		a.config.ShardSize,
		a.config.HTTPListenAddr,
		a.config.GRPCListenAddr)

	dexer.StopBlockNum = a.config.StopBlock
	dexer.Verbose = a.config.IsVerbose

	ctx, cancel := context.WithCancel(context.Background())
	a.OnTerminating(func(_ error) { cancel() })

	zlog.Info("resolving start block...")
	var targetStartBlockNum, filesourceStartBlockNum uint64
	var previousIrreversibleID string

	for {
		var e error
		targetStartBlockNum, filesourceStartBlockNum, previousIrreversibleID, e = a.resolveStartBlock(ctx, dexer)
		if e != nil {
			zlog.Warn("failed to resolve start block, retrying", zap.Error(e))
			time.Sleep(2 * time.Second)
			continue
		}
		zlog.Info("done resolving start block", zap.Uint64("target_start_block_num", targetStartBlockNum), zap.Uint64("filesource_start_block_num", filesourceStartBlockNum))
		break
	}

	if a.config.EnableBatchMode {
		dexer.BuildBatchPipeline(targetStartBlockNum, filesourceStartBlockNum, previousIrreversibleID, a.config.EnableUpload, a.config.DeleteAfterUpload)
	} else {
		dexer.BuildLivePipeline(targetStartBlockNum, filesourceStartBlockNum, previousIrreversibleID, a.config.EnableUpload, a.config.DeleteAfterUpload)
	}

	if a.config.EnableIndexTruncation {
		blockCount, err := getBlockCount(a.config.StartBlock)
		if err != nil {
			return fmt.Errorf("cannot setup moving tail: %w", err)
		}
		truncator := indexer.NewTruncator(dexer, blockCount)
		go truncator.Launch()
	}

	err = dexer.Bootstrap(targetStartBlockNum)
	if err != nil {
		return fmt.Errorf("failed to bootstrap indexer: %w", err)
	}

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	a.OnTerminating(dexer.Shutdown)
	dexer.OnTerminated(a.Shutdown)

	zlog.Info("launching indexer")
	go dexer.Launch()

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
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}

func getBlockCount(startBlock int64) (uint64, error) {
	if startBlock >= 0 {
		return 0, fmt.Errorf("start block %d must be a relative value (-) to yield a block count", startBlock)
	}
	return uint64(-1 * startBlock), nil
}
