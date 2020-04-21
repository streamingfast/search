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
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/indexer"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	HTTPListenAddr                      string // path for http healthcheck
	GRPCListenAddr                      string // path for gRPC healthcheck
	IndicesStoreURL                     string // Path to upload the wirtten index shards
	BlocksStoreURL                      string // Path to read blocks archives
	BlockstreamAddr                     string // gRPC URL to reach a stream of blocks
	WritablePath                        string // Writable base path for storing index files
	ShardSize                           uint64 // Number of blocks to store in a given Bleve index
	StartBlock                          int64  // Start indexing from block num
	StopBlock                           uint64 // Stop indexing at block num
	IsVerbose                           bool   // verbose logging
	EnableBatchMode                     bool   // Enabled the indexer in batch mode with a start & stop block
	BlockmetaAddr                       string // Blockmeta endpoint is queried to find the last irreversible block on the network being indexed
	NumberOfBlocksToFetchBeforeStarting uint64 // Number of blocks to fetch before start block
	EnableUpload                        bool   // Upload merged indexes to the --indexes-store
	DeleteAfterUpload                   bool   // Delete local indexes after uploading them
	EnableIndexTruncation               bool   // Enable index truncation, requires a relative --start-block (negative number)
}

type Modules struct {
	BlockMapper search.BlockMapper
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

func (a *App) Run() error {
	zlog.Info("running indexer app ", zap.Reflect("config", a.config))
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

	zlog.Info("setting up indexer")
	dexer := indexer.NewIndexer(
		indexesStore,
		blocksStore,
		a.config.BlockstreamAddr,
		a.modules.BlockMapper,
		a.config.WritablePath,
		a.config.ShardSize,
		a.config.HTTPListenAddr,
		a.config.GRPCListenAddr)

	dexer.StopBlockNum = a.config.StopBlock
	dexer.Verbose = a.config.IsVerbose

	effectiveStartBlockNum := uint64(a.config.StartBlock)
	if !a.config.EnableBatchMode {
		resolvedStartBlock := uint64(a.config.StartBlock)
		if a.config.StartBlock < 0 {
			zlog.Info("setting up head info client")
			conn, err := dgrpc.NewInternalClient(a.config.BlockstreamAddr)
			if err != nil {
				return fmt.Errorf("getting headinfo client: %w", err)
			}

			headinfoCli := pbheadinfo.NewHeadInfoClient(conn)
			libRef, err := search.GetLibInfo(headinfoCli)
			if err != nil {
				return fmt.Errorf("fetching LIB with headinfo: %w", err)
			}

			resolvedStartBlock = uint64(int64(libRef.Num()) + a.config.StartBlock)
			zlog.Info("indexer resolving start block block",
				zap.String("lib_id", libRef.ID()),
				zap.Uint64("lib_num", libRef.Num()),
				zap.Uint64("start_block", resolvedStartBlock))
		}

		indexesStore.SetOperationTimeout(90 * time.Second)
		effectiveStartBlockNum = dexer.NextBaseBlockAfter(resolvedStartBlock)
		zlog.Info("indexer live mode",
			zap.Int64("start_block_num", a.config.StartBlock),
			zap.Uint64("resolved_start_block_num", resolvedStartBlock),
			zap.Uint64("effective_start_block_num", effectiveStartBlockNum))
	}

	libNum := effectiveStartBlockNum
	if libNum > 0 {
		libNum -= 1
	}

	zlog.Info("setting up blockmeta")
	conn, err := dgrpc.NewInternalClient(a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("getting blockmeta client: %w", err)
	}
	blockmetaCli := pbblockmeta.NewBlockIDClient(conn)

	zlog.Info("getting irreversible block", zap.Uint64("lib_num", libNum))

	lastIrrBlockRef, err := a.getIrrBlock(blockmetaCli, libNum)
	if err != nil {
		if err == IndexerAppStartAborted {
			return nil
		}
		return fmt.Errorf("fetching irreversible block: %w", err)
	}

	zlog.Info("base irreversible block to start with",
		zap.Uint64("lib_num", lastIrrBlockRef.Num()),
		zap.String("lib_id", lastIrrBlockRef.ID()), zap.
			Uint64("effective_start_block", effectiveStartBlockNum))

	if a.config.EnableBatchMode {
		zlog.Info("setting up indexing batch pipeline")
		dexer.BuildBatchPipeline(lastIrrBlockRef, effectiveStartBlockNum, a.config.NumberOfBlocksToFetchBeforeStarting, a.config.EnableUpload, a.config.DeleteAfterUpload)
	} else {
		zlog.Info("setting up indexing live pipeline")
		dexer.BuildLivePipeline(lastIrrBlockRef, a.config.EnableUpload, a.config.DeleteAfterUpload)
	}

	if a.config.EnableIndexTruncation {
		blockCount, err := getBlockCount(a.config.StartBlock)
		if err != nil {
			return fmt.Errorf("cannot setup moving tail: %w", err)
		}
		truncator := indexer.NewTruncator(dexer, blockCount)
		go truncator.Launch()
	}

	err = dexer.Bootstrap(uint64(a.config.StartBlock))
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
		zlog.Info("readiness probe error", zap.Error(err))
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}

func (a *App) getIrrBlock(blockmetaCli pbblockmeta.BlockIDClient, blockNum uint64) (bstream.BlockRef, error) {
	for {
		lastIrrBlockRef, err := search.GetIrreversibleBlock(blockmetaCli, blockNum, context.Background(), 0)
		if err == nil {
			return lastIrrBlockRef, nil
		}

		select {
		case <-time.After(time.Second):
		case <-a.Shutter.Terminating():
			return nil, IndexerAppStartAborted
		}
	}
}

func getBlockCount(startBlock int64) (uint64, error) {
	if startBlock >= 0 {
		return 0, fmt.Errorf("start block %d must be a relative value (-) to yield a block count", startBlock)
	}
	return uint64(-1 * startBlock), nil
}
