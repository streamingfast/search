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
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// we need to be able to launch it with Start and Stop Block
// we simply a start block

type Indexer struct {
	*shutter.Shutter

	httpListenAddr string
	grpcListenAddr string

	StartBlockNum uint64
	StopBlockNum  uint64
	shardSize     uint64

	pipeline *Pipeline
	source   bstream.Source

	indexesStore    dstore.Store
	blocksStore     dstore.Store
	blockstreamAddr string
	blockFilter     func(blk *bstream.Block) error
	blockMapper     search.BlockMapper

	dfuseHooksActionName string
	writePath            string
	Verbose              bool

	ready        bool
	shuttingDown *atomic.Bool

	// Head block time, solely used to report drift
	headBlockTimeLock sync.RWMutex
	headBlockTime     time.Time

	libBlockLock sync.RWMutex
	libBlock     *bstream.Block
}

func NewIndexer(
	indexesStore dstore.Store,
	blocksStore dstore.Store,
	blockstreamAddr string,
	blockFilter func(blk *bstream.Block) error,
	blockMapper search.BlockMapper,
	writePath string,
	shardSize uint64,
	httpListenAddr string,
	grpcListenAddr string,

) *Indexer {
	indexer := &Indexer{
		Shutter:         shutter.New(),
		shuttingDown:    atomic.NewBool(false),
		indexesStore:    indexesStore,
		blocksStore:     blocksStore,
		blockstreamAddr: blockstreamAddr,
		blockFilter:     blockFilter,
		blockMapper:     blockMapper,
		shardSize:       shardSize,
		writePath:       writePath,
		httpListenAddr:  httpListenAddr,
		grpcListenAddr:  grpcListenAddr,
	}

	return indexer
}

func (i *Indexer) setReady() {
	i.ready = true
}

func (i *Indexer) isReady() bool {
	return i.ready
}

func (i *Indexer) Bootstrap(startBlockNum uint64) error {
	zlog.Info("bootstrapping from start blocknum", zap.Uint64("indexer_startblocknum", startBlockNum))
	i.StartBlockNum = startBlockNum
	if i.StartBlockNum%i.shardSize != 0 && i.StartBlockNum != 1 {
		return fmt.Errorf("indexer only starts RIGHT BEFORE the index boundaries, did you specify an irreversible block_id with a round number? It says %d", i.StartBlockNum)
	}
	return i.pipeline.Bootstrap(i.StartBlockNum)
}

func (i *Indexer) BuildLivePipeline(targetStartBlockNum, fileSourceStartBlockNum uint64, previousIrreversibleID string, enableUpload bool, deleteAfterUpload bool) {
	zlog.Info("setting up indexing live pipeline",
		zap.Uint64("target_start_block_num", targetStartBlockNum),
		zap.Uint64("file_source_start_block_num", fileSourceStartBlockNum),
		zap.String("previous_irreversible_id", previousIrreversibleID),
		zap.Bool("enable_upload", enableUpload),
	)
	pipe := i.newPipeline(i.blockMapper, enableUpload, deleteAfterUpload)

	var preprocessor bstream.PreprocessFunc
	if i.blockFilter != nil {
		preprocessor = bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
			return nil, i.blockFilter(blk)
		})
	}

	firstCall := true
	sf := bstream.SourceFromRefFactory(func(startBlockRef bstream.BlockRef, h bstream.Handler) bstream.Source {
		pipe.SetCatchUpMode()

		var handler bstream.Handler
		var startBlockNum uint64

		jsOptions := []bstream.JoiningSourceOption{bstream.JoiningSourceLogger(zlog)}
		zlog.Info("new source from ref factory",
			zap.Stringer("start_block", startBlockRef),
		)
		if firstCall {
			zlog.Info("first call setting up source from ref factory")
			firstCall = false
			startBlockNum = fileSourceStartBlockNum
			handler = h
		} else {
			zlog.Info("re-setting up source from ref factory")
			startBlockNum = startBlockRef.Num()
			handler = bstream.NewBlockIDGate(startBlockRef.ID(), bstream.GateExclusive, h, bstream.GateOptionWithLogger(zlog))
			jsOptions = append(jsOptions, bstream.JoiningSourceTargetBlockID(startBlockRef.ID()))
		}

		liveSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			source := blockstream.NewSource(
				context.Background(),
				i.blockstreamAddr,
				250,
				subHandler,
				blockstream.WithRequester("search-indexer"),
				blockstream.WithParallelPreproc(preprocessor, 2), // ok to give nil preprocessor ;)
			)
			return source
		})

		fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			fs := bstream.NewFileSource(
				i.blocksStore,
				startBlockNum,
				2, // always 2 download threads, ever
				preprocessor,
				subHandler,
			)
			return fs
		})

		protocolFirstBlock := bstream.GetProtocolFirstStreamableBlock
		if protocolFirstBlock > 0 {
			jsOptions = append(jsOptions, bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock))
		}

		// TODO: make the hard-coded `200` customizatble... or using a ChainConfig where the number of blocks represents a certain duration or something
		jsOptions = append(jsOptions, bstream.JoiningSourceLiveTracker(200, bstream.StreamHeadBlockRefGetter(i.blockstreamAddr)))

		return bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, handler, jsOptions...)
	})

	options := []forkable.Option{
		forkable.WithLogger(zlog),
		forkable.WithFilters(forkable.StepNew | forkable.StepIrreversible),
	}
	if previousIrreversibleID != "" {
		libRef := bstream.NewBlockRef(previousIrreversibleID, fileSourceStartBlockNum)
		zlog.Info("setting forkable inclusive lib options", zap.Stringer("lib", libRef))
		options = append(options, forkable.WithInclusiveLIB(libRef))
	}

	zlog.Info("initiating forkable irreversible block num gate", zap.Uint64("block_num", targetStartBlockNum))
	gate := forkable.NewIrreversibleBlockNumGate(targetStartBlockNum, bstream.GateInclusive, pipe, bstream.GateOptionWithLogger(zlog))

	forkableHandler := forkable.New(gate, options...)

	// note the indexer will listen for the source shutdown signal within the Launch() function
	// hence we do not need to propagate the shutdown signal originating from said source to the indexer. (i.e es.OnTerminating(....))
	es := bstream.NewEternalSource(sf, forkableHandler, bstream.EternalSourceWithLogger(zlog))

	i.source = es
	i.pipeline = pipe
}

func (i *Indexer) BuildBatchPipeline(targetStartBlockNum, fileSourceStartBlockNum uint64, previousIrreversibleID string, enableUpload bool, deleteAfterUpload bool) {
	zlog.Info("setting up indexing batch pipeline",
		zap.Uint64("target_start_block_num", targetStartBlockNum),
		zap.Uint64("filesource_start_block_num", fileSourceStartBlockNum),
		zap.String("previous_irreversible_id,", previousIrreversibleID),
		zap.Bool("enable_upload,", enableUpload),
		zap.Bool("delete_after_upload,", deleteAfterUpload),
	)

	pipe := i.newPipeline(i.blockMapper, enableUpload, deleteAfterUpload)

	gate := bstream.NewBlockNumGate(targetStartBlockNum, bstream.GateInclusive, pipe, bstream.GateOptionWithLogger(zlog))
	gate.MaxHoldOff = 0

	options := []forkable.Option{
		forkable.WithLogger(zlog),
		forkable.WithFilters(forkable.StepIrreversible),
	}

	if previousIrreversibleID != "" {
		options = append(options, forkable.WithInclusiveLIB(bstream.NewBlockRef(previousIrreversibleID, fileSourceStartBlockNum)))
	}

	forkableHandler := forkable.New(gate, options...)

	mapperPreproc := search.AsPreprocessBlock(pipe.mapper)
	filePreprocessor := bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
		if i.blockFilter != nil {
			err := i.blockFilter(blk)
			if err != nil {
				return nil, fmt.Errorf("block filter: %w", err)
			}
		}

		return mapperPreproc(blk)
	})

	fs := bstream.NewFileSource(
		i.blocksStore,
		fileSourceStartBlockNum,
		2,
		filePreprocessor,
		forkableHandler,
		bstream.FileSourceWithLogger(zlog),
	)

	// note the indexer will listen for the source shutdown signal within the Launch() function
	// hence we do not need to propagate the shutdown signal originating from said source to the indexer. (i.e fs.OnTerminating(....))
	i.source = fs
	i.pipeline = pipe
	pipe.SetCatchUpMode()
}

func (i *Indexer) Launch() {
	i.OnTerminating(func(e error) {
		zlog.Info("shutting down indexer's source") // TODO: triple check that we want to shutdown the source. PART OF A MERGE where intent is not clear.
		i.source.Shutdown(e)
		zlog.Info("shutting down indexer", zap.Error(e))
		i.cleanup()
	})

	i.serveHealthz()
	zlog.Info("launching pipeline")
	i.source.Run()

	if err := i.source.Err(); err != nil {
		if strings.HasSuffix(err.Error(), CompletedError.Error()) { // I'm so sorry, it is wrapped somewhere in bstream
			zlog.Info("Search Indexing completed successfully")
			i.Shutdown(nil)
			return
		}

		zlog.Error("search indexer source terminated with error", zap.Error(err))
	}

	i.Shutdown(i.source.Err())
	return
}

func (i *Indexer) cleanup() {
	zlog.Info("cleaning up indexer")
	i.shuttingDown.Store(true)

	zlog.Info("waiting on uploads")
	i.pipeline.WaitOnUploads()

	zlog.Sync()
	zlog.Info("indexer shutdown complete")
}
