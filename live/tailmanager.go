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
	"fmt"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dmesh"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search"
	"github.com/streamingfast/search/metrics"
	"go.uber.org/zap"
)

// TailManager truncates the buffer under the guard of the
// LowBlockGuard (populated by the SubscriptionHub), as well as
// managing which lower block is published on dmesh, to slowly drain
// our requests that we're about to truncate.. then truncates the
// buffer, and cleans up the bleve indexes below.
type TailManager struct {
	sync.Mutex

	searchPeer          *dmesh.SearchPeer
	dmeshClient         dmeshClient.Client
	tailLock            *bstream.TailLock
	getSearchPeers      GetSearchPeersFunc
	buffer              *bstream.Buffer
	minTargetBufferSize int
	backendThreshold    int
	currentLIB          bstream.BlockRef
	currentLIBMutex     sync.RWMutex
	targetLIB           bstream.BlockRef // useful for readiness check, will set "CurrentLIB" to that block when it sees it in the buffer
}

func NewTailManager(getSearchPeers GetSearchPeersFunc, dmeshClient dmeshClient.Client, searchPeer *dmesh.SearchPeer, buffer *bstream.Buffer, minTargetBufferSize int, backendThreshold int, targetLIB bstream.BlockRef) *TailManager {
	return &TailManager{
		targetLIB:           targetLIB,
		dmeshClient:         dmeshClient,
		searchPeer:          searchPeer,
		tailLock:            bstream.NewTailLock(),
		getSearchPeers:      getSearchPeers,
		buffer:              buffer,
		minTargetBufferSize: minTargetBufferSize,
		backendThreshold:    backendThreshold,
	}

}

func (t *TailManager) Ready() bool {
	return t.CurrentLIB() != nil
}

func (t *TailManager) TailLock(startBlockNum uint64) (releaseFunc func(), err error) {
	t.Lock()
	defer t.Unlock()

	zlog.Debug("trying to lock tail", zap.Uint64("start_block_num", startBlockNum))
	if !t.buffer.Contains(startBlockNum) {
		var printNums []uint64
		for _, blk := range t.buffer.AllBlocks() {
			printNums = append(printNums, blk.Num())
		}
		zlog.Debug("here are all blocks nums", zap.Any("all_blocks_nums", printNums))
		return nil, fmt.Errorf("buffer doesn't contain %d", startBlockNum)
	}

	return t.tailLock.TailLock(startBlockNum), nil
}

func (t *TailManager) Launch() {
	zlog.Info("launching tailmanager")
	for {
		if !t.Ready() {
			t.setInitialLIBTail()
			time.Sleep(time.Second)
			continue
		}
		t.attemptTruncation()
		time.Sleep(5 * time.Second)
	}
}

func (t *TailManager) setInitialLIBTail() {
	t.Lock()
	defer t.Unlock()
	zlog.Info("trying to set initial publishing buffer tail", zap.String("target_lib_id", t.targetLIB.ID()), zap.Uint64("target_lib_num", t.targetLIB.Num()))
	allBlocks := t.buffer.AllBlocks()
	if len(allBlocks) == 0 {
		zlog.Info("no blocks in buffer, waiting before setting initial lib")
		return
	}
	for _, blk := range allBlocks {
		if (blk.ID() == t.targetLIB.ID()) || t.targetLIB.Num() < 3 {
			zlog.Info("setting initial publishing buffer tail and currentLIB", zap.String("target_lib_id", t.targetLIB.ID()), zap.Uint64("target_lib_num", t.targetLIB.Num()))
			t.setCurrentLIB(t.targetLIB)
			t.publishTailBlock(blk.Num(), blk.ID())
			return
		}
	}
	zlog.Info("couldn't find target lib in buffer between head and tail",
		zap.Uint64("head_num", allBlocks[len(allBlocks)-1].Num()),
		zap.Uint64("tail_num", allBlocks[0].Num()))
	return
}

func (t *TailManager) publishTailBlock(blockNum uint64, blockID string) {
	zlog.Info("publishing tail block to dmesh", zap.Uint64("block_num", blockNum), zap.String("block_id", blockID))
	metrics.TailBlockNumber.SetUint64(blockNum)
	t.searchPeer.Locked(func() {
		t.searchPeer.TailBlock = blockNum
		t.searchPeer.TailBlockID = blockID
	})
	err := t.dmeshClient.PublishNow(t.searchPeer)
	if err != nil {
		zlog.Error("error publishing to dmesh", zap.Error(err))
	}
}

func (t *TailManager) CurrentLIB() bstream.BlockRef {
	t.currentLIBMutex.RLock()
	defer t.currentLIBMutex.RUnlock()
	return t.currentLIB
}

func (t *TailManager) setCurrentLIB(lib bstream.BlockRef) {
	t.currentLIBMutex.Lock()
	defer t.currentLIBMutex.Unlock()
	t.currentLIB = lib
}

func (t *TailManager) attemptTruncation() {
	t.Lock()
	defer t.Unlock()

	if t.buffer.Len() < t.minTargetBufferSize {
		return
	}

	publishedTailNum := t.searchPeer.TailBlock
	publishedTailID := t.searchPeer.TailBlockID
	bufferTail := t.buffer.Tail()
	zlog.Debug("attempting truncation", zap.Uint64("published_tail", publishedTailNum), zap.Uint64("buffer_tail_num", bufferTail.Num()))

	if bufferTail.Num() != publishedTailNum { // next pass
		tailLockBound := t.tailLock.LowerBound()
		if tailLockBound != 0 && publishedTailNum > tailLockBound {
			zlog.Warn("warning: we tail locked blocks lower than what we published, can't do that for long")
			return
		}

		zlog.Debug("truncating below", zap.Uint64("published_tail", publishedTailNum))
		t.setCurrentLIB(bstream.NewBlockRef(publishedTailID, publishedTailNum))
		t.truncateBelow(publishedTailNum)
	}

	targetTruncateBlock := GetMeshLIB(t.getSearchPeers, t.backendThreshold)
	if targetTruncateBlock == nil {
		zlog.Info("still don't have a target truncation block, backends don't meet threshold", zap.Int("threshold", t.backendThreshold))
		return
	}

	if targetTruncateBlock.Num() != publishedTailNum {
		t.publishTailBlock(targetTruncateBlock.Num(), targetTruncateBlock.ID())
	}
}

func (t *TailManager) truncateBelow(blockNum uint64) {
	refs := t.buffer.TruncateTail(blockNum - 1)

	go func() {
		time.Sleep(3 * time.Minute)
		zlog.Debug("deleting a few live indexes", zap.Int("count", len(refs)))
		for _, ref := range refs {
			preprocBlk := ref.(*bstream.PreprocessedBlock)
			i := preprocBlk.Obj.(*search.SingleIndex)
			i.Delete()
		}
	}()
}
