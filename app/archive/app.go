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
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/streamingfast/derr"
	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/dstore"
	pbheadinfo "github.com/streamingfast/pbgo/dfuse/headinfo/v1"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/dmesh"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search"
	"github.com/streamingfast/search/archive"
	"github.com/streamingfast/search/archive/roarcache"
	"github.com/streamingfast/search/metrics"
	"go.uber.org/zap"
)

type Config struct {
	// dmesh configuration
	ServiceVersion          string        // dmesh service version (v1)
	TierLevel               uint32        // level of the search tier
	GRPCListenAddr          string        // Address to listen for incoming gRPC requests
	HTTPListenAddr          string        // Address to listen for incoming http requests
	PublishInterval         time.Duration // longest duration a dmesh peer will not publish
	EnableMovingTail        bool          // Enable moving t`ail, requires a relative --start-block (negative number)
	IndexesStoreURL         string        // location of indexes to download/open/serve
	IndexesPath             string        // location where to store the downloaded index files
	ReadOnlyIndexesPaths    []string      // list of paths where to load indexes on start
	ShardSize               uint64        // indexes shard size
	StartBlock              int64         // Start at given block num, the initial sync and polling
	StopBlock               uint64        // Stop before given block num, the initial sync and polling
	BlockmetaAddr           string        // grpc address to blockmeta to establish negative start block
	SyncFromStore           bool          // Download missing indexes from --indexes-store before starting
	SyncMaxIndexes          int           // Maximum number of indexes to sync. On production, use a very large number.
	IndicesDLThreads        int           // Number of indices files to download from the GS input store and decompress in parallel. In prod, use large value like 20.
	NumQueryThreads         int           // Number of end-user query parallel threads to query blocks indexes
	IndexPolling            bool          // Populate local indexes using indexes store polling.
	WarmupFilepath          string        // Optional filename containing queries to warm-up the search
	ShutdownDelay           time.Duration //On shutdown, time to wait before actually leaving, to try and drain connections
	EnableEmptyResultsCache bool          // Enable roaring-bitmap-based empty results caching
	MemcacheAddr            string        // Empty results cache's memcache server address
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
	zlog.Info("running archive app ", zap.Reflect("config", a.config))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metrics.Register(metrics.ArchiveMetricsSet)

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

	var cache roarcache.Cache
	if a.config.EnableEmptyResultsCache {
		zlog.Info("setting up roar cache")
		cache = roarcache.NewMemcache(a.config.MemcacheAddr, 30*24*time.Hour, a.config.ShardSize)
	}

	zlog.Info("creating search peer")
	movingHead := a.config.StopBlock == 0
	searchPeer := dmesh.NewSearchArchivePeer(a.config.ServiceVersion, a.config.GRPCListenAddr, a.config.EnableMovingTail, movingHead, a.config.ShardSize, a.config.TierLevel, a.config.PublishInterval)

	zlog.Info("publishing search archive peer", zap.String("peer_host", searchPeer.GenericPeer.Host))
	err = a.modules.Dmesh.PublishNow(searchPeer)
	if err != nil {
		return fmt.Errorf("publishing peer to dmesh: %w", err)
	}

	resolvedStartBlockNum, err := resolveStartBlock(ctx, a.config.StartBlock, a.config.ShardSize, a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("cannot resolve start block num: %w", err)
	}
	zlog.Info("start block num resolved",
		zap.Int64("start_block", a.config.StartBlock),
		zap.Uint64("shard_size", a.config.ShardSize),
		zap.Uint64("resolved_start_block_num", resolvedStartBlockNum))

	var blockCount uint64
	if a.config.EnableMovingTail {
		blockCount, err = getBlockCount(a.config.StartBlock)
		derr.Check("cannot setup moving tail", err)
	}

	indexesStore, err := dstore.NewStore(a.config.IndexesStoreURL, "", "zstd", true)
	if err != nil {
		return fmt.Errorf("failed setting up indexes store: %w", err)
	}

	zlog.Info("setting up scorch index pool")
	indexPool, err := archive.NewIndexPool(
		a.config.IndexesPath,
		a.config.ReadOnlyIndexesPaths,
		a.config.ShardSize,
		indexesStore,
		cache,
		a.modules.Dmesh,
		searchPeer,
	)

	zlog.Info("cleaning on-disk indexes")
	err = indexPool.CleanOnDiskIndexes(resolvedStartBlockNum, a.config.StopBlock)
	if err != nil {
		return fmt.Errorf("cleaning on-disk indexes: %w", err)
	}

	if a.config.SyncFromStore {
		zlog.Info("sync'ing from storage")
		err := indexPool.SyncFromStorage(resolvedStartBlockNum, a.config.StopBlock, a.config.SyncMaxIndexes, a.config.IndicesDLThreads)
		if err != nil {
			return fmt.Errorf("syncing from storage: %w", err)
		}
	}

	zlog.Info("loading on-disk indexes")
	err = indexPool.ScanOnDiskIndexes(resolvedStartBlockNum)
	if err != nil {
		return fmt.Errorf("opening read-only indexes: %w", err)
	}

	err = indexPool.SetLowestServeableBlockNum(resolvedStartBlockNum)
	if err != nil {
		return fmt.Errorf("setting lowest serveable block num: %w", err)
	}

	lastIrrBlockNum := indexPool.LastReadOnlyIndexedBlock()
	if lastIrrBlockNum == 0 && a.config.StartBlock != 0 {
		lastIrrBlockNum = resolvedStartBlockNum - 1
	}

	lastIrrBlockID := indexPool.LastReadOnlyIndexedBlockID()

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

	if a.config.EnableMovingTail {
		truncator := archive.NewTruncator(indexPool, blockCount)
		go truncator.Launch()
	}

	if a.config.IndexPolling {
		go indexPool.PollRemoteIndices(resolvedStartBlockNum, a.config.StopBlock)
	}

	zlog.Info("setting up archive backend")
	archiveBackend := archive.NewBackend(indexPool, a.modules.Dmesh, searchPeer, a.config.GRPCListenAddr, a.config.HTTPListenAddr, a.config.ShutdownDelay)
	archiveBackend.SetMaxQueryThreads(a.config.NumQueryThreads)

	if a.config.WarmupFilepath != "" {
		err := warmupSearch(a.config.WarmupFilepath, indexPool.GetLowestServeableBlockNum(), indexPool.LastReadOnlyIndexedBlock(), archiveBackend)
		if err != nil {
			return fmt.Errorf("unable to warmup search: %w", err)
		}
	}

	if !indexPool.IsEmpty() {
		err = indexPool.SetReady()
		if err != nil {
			return fmt.Errorf("setting ready: %w", err)
		}
	}

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	a.OnTerminating(func(e error) {
		zlog.Info("archive application is terminating, shutting down archive backend")
		archiveBackend.Shutdown(e)
		zlog.Info("archive backend shutdown complete")
	})
	archiveBackend.OnTerminated(func(e error) {
		zlog.Info("archive backend terminated , shutting down archive application")
		a.Shutdown(e)
		zlog.Info("archive application shutdown complete")
	})

	zlog.Info("launching backend")
	go archiveBackend.Launch()

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

func resolveStartBlock(ctx context.Context, startBlock int64, shardSize uint64, blockmetaAddr string) (uint64, error) {
	if startBlock >= 0 {
		zlog.Info("resolving start block", zap.Int64("start_block", startBlock), zap.Uint64("shard_size", shardSize))
		if startBlock%int64(shardSize) != 0 {
			return 0, fmt.Errorf("start block %d misaligned with shard size %d", startBlock, shardSize)
		} else {
			return uint64(startBlock), nil
		}
	}

	zlog.Info("blockemta setup getting start block")
	conn, err := dgrpc.NewInternalClient(blockmetaAddr)
	if err != nil {
		return 0, fmt.Errorf("getting blockmeta headinfo client: %w", err)
	}
	headinfoCli := pbheadinfo.NewHeadInfoClient(conn)
	hi, err := headinfoCli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{
		Source: pbheadinfo.HeadInfoRequest_STREAM,
	})
	if err != nil {
		return 0, fmt.Errorf("getting blockmeta headinfo: %w", err)
	}
	zlog.Info("resolving start block", zap.Int64("start_block", startBlock), zap.Uint64("shard_size", shardSize), zap.Uint64("irr_block_num", hi.LibNum))

	absoluteStartBlock := (int64(hi.LibNum) + startBlock)
	absoluteStartBlock = absoluteStartBlock - (absoluteStartBlock % int64(shardSize))
	if absoluteStartBlock < 0 {
		return 0, fmt.Errorf("relative start block %d  is to large, cannot resolve to a negative start block %d", startBlock, absoluteStartBlock)
	}
	return uint64(absoluteStartBlock), nil
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
