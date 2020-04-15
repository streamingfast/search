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
	"crypto/md5"
	"fmt"
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/archive/roarcache"
	"github.com/dfuse-io/search/metrics"
	"go.uber.org/zap"
	"google.golang.org/appengine/memcache"
)

type indexIterator struct {
	pool *IndexPool

	roar      *roaring.Bitmap
	roarCache roarcache.Cache
	roarKey   string
	roarLock  sync.RWMutex
	roarDirty bool

	readPoolStartBlock uint64
	readPoolSnapshot   []*search.ShardIndex

	startBlock   uint64
	endBlock     uint64
	currentBlock uint64
	shardSize    uint64

	sortDesc bool
}

func (p *IndexPool) GetIndexIterator(lowBlockNum, highBlockNum uint64, sortDesc bool) (*indexIterator, error) {
	p.readPoolLock.RLock()
	defer p.readPoolLock.RUnlock()

	if lowBlockNum < p.LowestServeableBlockNum {
		return nil, fmt.Errorf("range of query out of bounds: requested low: %d, start block from this archive: %d", lowBlockNum, p.LowestServeableBlockNum)
	}

	it := &indexIterator{
		pool:               p,
		sortDesc:           sortDesc,
		shardSize:          p.ShardSize,
		readPoolStartBlock: p.LowestServeableBlockNum,
		readPoolSnapshot:   p.ReadPool,
		roarCache:          p.emptyResultsCache,
	}

	if sortDesc {
		it.startBlock = highBlockNum
		it.endBlock = lowBlockNum
	} else {
		it.startBlock = lowBlockNum
		it.endBlock = highBlockNum
	}
	it.currentBlock = it.startBlock

	return it, nil
}

func (it *indexIterator) LoadRoaring(rawQuery string) {
	it.roar = roaring.New()
	it.roarKey = fmt.Sprintf("%x", md5.Sum([]byte(rawQuery)))
	zlog.Debug("loading roaring", zap.String("raw_query", rawQuery), zap.String("md5sum", it.roarKey))

	err := it.roarCache.Get(it.roarKey, it.roar)
	if err != nil {
		if err.Error() == memcache.ErrCacheMiss.Error() {
			zlog.Debug("cache miss", zap.String("md5sum", it.roarKey))
			metrics.RoarCacheMiss.Inc()
			return
		}
		zlog.Error("failed getting roaring bitmap key from cache", zap.Error(err))
		metrics.RoarCacheFail.Inc()
		return
	}
	metrics.RoarCacheHit.Inc()
	return
}

// OptimizeAndPublishRoaring does what it says.  It expects the
// Iterator not to be used any more, as it writes to the
// roaring.Bitmap.
func (it *indexIterator) OptimizeAndPublishRoaring() {
	if it.roar == nil || !it.roarDirty {
		return
	}

	go func() {
		zlog.Debug("saving roaring", zap.String("md5sum", it.roarKey))
		it.roar.RunOptimize()
		if err := it.roarCache.Put(it.roarKey, it.roar); err != nil {
			zlog.Error("failed writing roaring bitmap to cache", zap.Error(err))
		}
	}()
}

func (it *indexIterator) MarkEmpty(idxStartBlock uint64) {
	if it.roar == nil {
		return
	}

	it.roarLock.Lock()
	defer it.roarLock.Unlock()

	absoluteIndexNum := uint32(idxStartBlock / it.shardSize)
	//zlog.Debug("Adding to the bitmap", zap.Uint32("abs_index_num", absoluteIndexNum))
	it.roar.Add(absoluteIndexNum)
	it.roarDirty = true
}

func (it *indexIterator) CurrentBase() uint64 {
	return it.currentBlock
}

func noop() {}

// Next returns the next iterator from the initial `startBlock`.
// `release` is a function you MUST call when you are done, to release
// the read lock on the `shardIndex`, allowing it to be closed.  The
// only indexes we close are the `mergePool` and `writable`
// indexes. `release` is a noop for read-only indexes.
//
// Depending on the `sortDesc` direction, `Next()` will either move forward or backwards.
func (it *indexIterator) Next() (idx *search.ShardIndex, skipIndex bool, release func()) {
	release = noop
	if it.sortDesc && it.currentBlock < it.endBlock {
		return
	}
	if !it.sortDesc && it.currentBlock > it.endBlock {
		return
	}

	idx, release = it.current(it.currentBlock)
	if idx != nil {
		skipIndex = it.containedInRoaring(it.currentBlock)

		if it.sortDesc {
			if idx.StartBlock == 0 {
				it.currentBlock = math.MaxUint64
				// Ensure we stop after
			} else {
				it.currentBlock = idx.StartBlock - 1
			}
		} else {
			it.currentBlock = idx.EndBlock + 1
		}
	}
	return
}

func (it *indexIterator) containedInRoaring(currentBlock uint64) bool {
	if it.roar == nil {
		return false
	}

	it.roarLock.RLock()
	defer it.roarLock.RUnlock()

	absoluteIndexNum := uint32(currentBlock / it.shardSize)
	if it.roar.Contains(absoluteIndexNum) {
		metrics.RoarCacheHitIndexesSkipped.Inc()
		return true
	}
	return false
}

// current returns the index that contains the `currentBlock`, no matter which type or state
// it is.
func (it *indexIterator) current(currentBlock uint64) (idx *search.ShardIndex, releaseFunc func()) {
	p := it.pool

	if it.readPoolStartBlock > currentBlock {
		return nil, nil
	}
	if currentBlock == math.MaxUint32 {
		return nil, nil
	}

	// Look into read-only indexes
	readonlySliceIndex := (currentBlock - it.readPoolStartBlock) / p.ShardSize
	if int(readonlySliceIndex) < len(it.readPoolSnapshot) {
		idx := it.readPoolSnapshot[readonlySliceIndex]
		return idx, noop
	}

	// Try to see if the real-time readPool has been updated in the mean time.
	p.readPoolLock.RLock()
	if len(p.ReadPool) > int(readonlySliceIndex) {
		idx = p.ReadPool[readonlySliceIndex]
	}
	p.readPoolLock.RUnlock()

	if idx != nil {
		return idx, noop
	}

	return nil, nil
}

func doneOnce(f func()) func() {
	var once sync.Once
	return func() { once.Do(f) }
}
