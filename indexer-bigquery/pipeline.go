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

package indexer_bigquery

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var CompletedError = fmt.Errorf("batch completed")

type pipelineMode int

const (
	unknownMode pipelineMode = iota
	catchUpMode
	liveMode
)

func (m pipelineMode) String() string {
	if m == catchUpMode {
		return "catch_up"
	}

	if m == liveMode {
		return "live"
	}

	return "unknown"
}

type Pipeline struct {
	mapper       *search.Mapper
	indexer      *IndexerBigQuery
	mode         *atomic.Int32
	catchUpStats *stats
	liveStats    *stats

	shardSize uint64

	// writable index is being filled with irreversible blocks, with the goal of compacting it later, and making it a new ShardSize shard.
	writable             *BigQueryShardIndex // should never be `nil` if started without `--no-write`
	writableLastBlockNum atomic.Uint64
	writableLastBlockID  atomic.String

	writablePath string //local path where indices are stored on disk
	indexesStore dstore.Store

	uploadGroup       sync.WaitGroup
}

func (i *IndexerBigQuery) newPipeline(blockMapper search.BlockMapper) *Pipeline {
	mapper, err := search.NewMapper(blockMapper)
	if err != nil {
		zlog.Panic(err.Error())
	}

	return &Pipeline{
		mapper:            mapper,
		indexer:           i,
		shardSize:         i.shardSize,
		writablePath:      i.writePath,
		indexesStore:      i.indexesStore,
		mode:              atomic.NewInt32(int32(unknownMode)),
		catchUpStats:      &stats{},
		liveStats:         &stats{},
	}
}

func (pipe *Pipeline) SetCatchUpMode() {
	pipe.mode.Store(int32(catchUpMode))
	pipe.catchUpStats.reset()
}

func (p *Pipeline) Bootstrap(startBlockNum uint64) error {
	idx, err := p.newWritableIndex(startBlockNum)
	if err != nil {
		return err
	}

	p.writable = idx
	return nil
}

func (p *Pipeline) WaitOnUploads() {
	time.Sleep(1 * time.Second)

	zlog.Info("waiting on merges to terminate")
	p.uploadGroup.Wait()
}

func (pipe *Pipeline) ProcessBlock(blk *bstream.Block, objWrap interface{}) error {
	obj := objWrap.(*forkable.ForkableObject)

	blockTime := blk.Time()

	//FIXME: Should wrap this in a check? make sure it is not alreay ready
	pipe.indexer.setReady()

	step := obj.Step

	switch step {
	case forkable.StepNew:

		// Just record the head block for healthz
		pipe.indexer.headBlockTimeLock.Lock()
		pipe.indexer.headBlockTime = blockTime
		pipe.indexer.headBlockTimeLock.Unlock()

		headBlockTimeDrift.SetBlockTime(blockTime)
		headBlockNumber.SetUint64(blk.Num())
		return nil

	case forkable.StepIrreversible:

		var docsList []map[string]interface{}
		if obj.Obj == nil { // was not preprocessed
			preprocessedObj, err := pipe.mapper.PreprocessBlock(blk)
			if err != nil {
				return err
			}
			docsList = preprocessedObj.([]map[string]interface{})
		} else {
			docsList = obj.Obj.([]map[string]interface{})
		}
		err := pipe.processIrreversibleBlock(blk, docsList)
		if err != nil {
			return err
		}

		// record it for truncation purposes
		pipe.indexer.libBlockLock.Lock()
		pipe.indexer.libBlock = blk
		pipe.indexer.libBlockLock.Unlock()

		// TODO: should we update metrics

		return nil

	default:
		return fmt.Errorf("unsupported step in forkable pipeline: %s", step)
	}
}

func (pipe *Pipeline) processIrreversibleBlock(blk *bstream.Block, docsList []map[string]interface{}) error {
	// TODO: when doing reprocessing, or when there's a stop-block, you don't need to
	//       write StepNew in Live blocks.

	blockID := blk.ID()
	blockNum := blk.Num()

	if pipe.indexer.StopBlockNum != 0 && blockNum > pipe.indexer.StopBlockNum {
		return CompletedError
	}

	currentMode := pipelineMode(pipe.mode.Load())
	stats := pipe.liveStats
	if currentMode == catchUpMode {
		stats = pipe.catchUpStats
	}

	stats.blockCount++
	stats.documentCount += uint64(len(docsList))

	secondsSinceStart := time.Since(stats.startTime).Seconds()
	blocksPerSecond := float64(stats.blockCount) / secondsSinceStart
	docsPerSecond := float64(stats.documentCount) / secondsSinceStart

	if currentMode == catchUpMode {
		metrics.CatchUpBlocksPerSecond.SetFloat64(blocksPerSecond)
		metrics.CatchUpDocsPerSecond.SetFloat64(docsPerSecond)
	}

	if blockNum%100 == 0 {
		zlog.Info("processing irreversible block",
			zap.Stringer("block", blk),
			zap.Float64("blocks_per_second", blocksPerSecond),
			zap.Float64("docs_per_second", docsPerSecond),
			zap.Stringer("mode", currentMode),
		)
	}

	if blockNum%pipe.shardSize == 0 && pipe.writableLastBlockID.Load() != "" {
		zlog.Info("rotating index right before this block", zap.Uint64("shard_size", pipe.shardSize), zap.Stringer("this_block", blk), zap.String("writeable_last_block_id", pipe.writableLastBlockID.Load()))
		if err := pipe.saveIndexFile(blockNum, blockID); err != nil {
			return err
		}
	}

	if blockNum == pipe.indexer.StopBlockNum {
		return CompletedError
	}

	if err := pipe.writeIrreversibleBatch(docsList, blockNum, blockID); err != nil {
		return err
	}

	return nil
}

func (p *Pipeline) buildWritableIndexFilePath(baseBlockNum uint64, suffix string) string {
	if suffix != "" {
		suffix = "-" + suffix
	}

	return filepath.Join(p.writablePath, fmt.Sprintf("%010d%s.avro", baseBlockNum, suffix))
}

func (p *Pipeline) newWritableIndex(baseBlockNum uint64) (*BigQueryShardIndex, error) {
	var err error
	var shardIndex *BigQueryShardIndex

	shardIndex, _ = NewBigQueryShardIndex(baseBlockNum, p.shardSize, p.buildWritableIndexFilePath) // error only happens when input index is not nil

	buildingPath := shardIndex.WritablePath("building")

	_ = os.RemoveAll(buildingPath)
	os.MkdirAll(buildingPath, 0755)

	shardIndex.IndexTargetPath = shardIndex.WritablePath("")
	if err != nil {
		return nil, fmt.Errorf("unable to create offline index builder: %s", err)
	}

	return shardIndex, nil
}

func (p *Pipeline) saveIndexFile(nextIndexBase uint64, currentBlockID string) (err error) {
	if p.writableLastBlockID.Load() == "" {
		// do not create a new one if there is not one
		return nil
	}

	currentIndexBaseBlock := nextIndexBase - p.shardSize

	zlog.Info("prepping new writable index", zap.Uint64("base", nextIndexBase))
	newWritable, err := p.newWritableIndex(nextIndexBase)
	if err != nil {
		return err
	}
	newWritable.EndBlock = 0

	previousWritable := p.writable
	p.writable = newWritable

	zlog.Info("uploading Index", zap.Uint64("base", currentIndexBaseBlock))

	go p.prepareBackgroundUpload(previousWritable)

	return nil
}

func (p *Pipeline) prepareBackgroundUpload(idx *BigQueryShardIndex) {
	p.uploadGroup.Add(1)

	// need to decrement uploadGroup counter *before* shutdown
	var propagateError = func(msg string, err error) {
		zlog.Error(msg, zap.Error(err))
		p.uploadGroup.Done()
		p.indexer.Shutdown(err)
	}

	// Clean it up before `Close()` writes to it.
	finalPath := idx.IndexTargetPath
	_ = os.RemoveAll(finalPath)

	t0 := time.Now()
	err := idx.Close()
	if err != nil {
		propagateError("error closing the offline index builder", err)
		return
	}
	zlog.Info("offline index builder closed", zap.Duration("timing", time.Since(t0)))

	buildingPath := idx.WritablePath("building")
	_ = os.RemoveAll(buildingPath)

	if err := p.Upload(idx.StartBlock, finalPath); err != nil {
		propagateError(fmt.Sprintf("upload failed, base: %d", idx.StartBlock), err)
		return
	}

	zlog.Info("upload: done", zap.Uint64("base", idx.StartBlock))

	zlog.Info("deleting processed files", zap.Uint64("base", idx.StartBlock))
	err = os.RemoveAll(finalPath)
	if err != nil {
		propagateError(fmt.Sprintf("cannot remove read path %q", finalPath), err)
		return
	}

	p.uploadGroup.Done()
	return
}

func (p *Pipeline) writeIrreversibleBatch(docsList []map[string]interface{}, blockNum uint64, blockID string) error {
	if p.writable == nil {
		return fmt.Errorf("no writable index ready in index pool")
	}

	p.writable.Lock.Lock()
	defer p.writable.Lock.Unlock()

	for _, doc := range docsList {
		if err := p.writable.Index(doc); err != nil {
			return fmt.Errorf("offline index builder Index: %w", err)
		}
	}

	p.writableLastBlockNum.Store(blockNum)
	p.writableLastBlockID.Store(blockID)
	p.writable.EndBlock = blockNum
	p.writable.EndBlockID = blockID

	metrics.LastWrittenBlockNumber.SetUint64(blockNum)

	return nil
}

type stats struct {
	blockCount    uint64
	documentCount uint64
	startTime     time.Time
}

func (s *stats) reset() {
	s.blockCount = 0
	s.documentCount = 0
	s.startTime = time.Now()
}
