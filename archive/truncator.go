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
	"time"

	"github.com/dfuse-io/search/metrics"
	"go.uber.org/zap"
)

type Truncator struct {
	indexPool  *IndexPool
	blockCount uint64

	targetTruncateBlock uint64
}

func NewTruncator(indexPool *IndexPool, blockCount uint64) *Truncator {
	return &Truncator{
		indexPool:  indexPool,
		blockCount: blockCount,
	}
}

func (t *Truncator) Launch() {
	zlog.Info("launching index truncator")
	for {
		time.Sleep(5 * time.Second)
		t.attemptTruncation()
	}
}

func (t *Truncator) attemptTruncation() {
	if t.shouldTruncate() {
		zlog.Info("truncating below",
			zap.Uint64("target_truncate_block", t.targetTruncateBlock),
			zap.Uint64("current_lowest_serveable_block", t.indexPool.GetLowestServeableBlockNum()))
		t.indexPool.truncateBelow(t.targetTruncateBlock)
		t.indexPool.SetLowestServeableBlockNum(t.targetTruncateBlock)
	}

	// getting the highest current read only block
	highestIndexedBlock := t.indexPool.LastReadOnlyIndexedBlock()
	lowestIndexedBlock := t.indexPool.GetLowestServeableBlockNum()

	if (highestIndexedBlock - lowestIndexedBlock) < t.blockCount {
		zlog.Debug("not enough blocks in pool to truncate",
			zap.Uint64("high_index_block_num", highestIndexedBlock),
			zap.Uint64("low_index_block_num", lowestIndexedBlock),
			zap.Uint64("block_count", t.blockCount))
		return
	}

	toPublish := t.indexPool.LowestServeableBlockNumAbove(highestIndexedBlock - t.blockCount) // a number ending in 999

	zlog.Debug("attempt truncation",
		zap.Uint64("high_index_block_num", highestIndexedBlock),
		zap.Uint64("low_index_block_num", lowestIndexedBlock),
		zap.Uint64("target_truncate_block", toPublish),
		zap.Uint64("block_count", t.blockCount))

	if toPublish >= t.indexPool.LowestServeableBlockNum {
		if err := t.publishTailBlock(toPublish); err != nil {
			zlog.Error("error publishing to dmesh", zap.Error(err))
			return
		}
		t.targetTruncateBlock = toPublish
		metrics.TailBlockNumber.SetUint64(toPublish)
	}
}

func (t *Truncator) shouldTruncate() bool {
	return t.indexPool.GetLowestServeableBlockNum() < t.targetTruncateBlock
}

func (t *Truncator) publishTailBlock(blockNum uint64) error {
	zlog.Info("publishing tail block to dmesh", zap.Uint64("block_num", blockNum))
	if searchPeer := t.indexPool.SearchPeer; searchPeer != nil {
		searchPeer.Locked(func() {
			searchPeer.TailBlock = blockNum
			// we do not actually need the tail block ID here, it is not used by anyone.
		})
		err := t.indexPool.dmeshClient.PublishNow(searchPeer)
		if err != nil {
			return err
		}
	}
	return nil
}
