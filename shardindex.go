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

package search

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/index"
	bsearch "github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collector"
	"github.com/blevesearch/bleve/search/query"
	"go.uber.org/zap"
)

func NewShardIndexWithAnalysisQueue(baseBlockNum uint64, shardSize uint64, idx index.Index, pathFunc filePathFunc, analysisQueue *index.AnalysisQueue) (*ShardIndex, error) {
	si, err := newShardIndex(baseBlockNum, shardSize, idx, pathFunc)
	if err != nil {
		return nil, err
	}
	si.analysisQueue = analysisQueue
	return si, nil
}

func newShardIndex(baseBlockNum uint64, shardSize uint64, idx index.Index, pathFunc filePathFunc) (*ShardIndex, error) {
	shard := &ShardIndex{
		StartBlock:                baseBlockNum,
		EndBlock:                  baseBlockNum + shardSize - 1,
		Index:                     idx,
		writableIndexFilePathFunc: pathFunc,
	}
	if baseBlockNum == 0 && shardSize == 0 {
		shard.EndBlock = 0
		return shard, nil
	}

	if idx != nil {
		start, end, err := shard.GetBoundaryBlocks(idx)
		if err != nil {
			return nil, err
		}

		if !isValidBoundaries(start, end) {
			return nil, fmt.Errorf("cannot create new shard: missing boundaries info in bleve shard")
		}

		shard.StartBlockTime = start.Time // this MAY be block 1 or 2, instead of expected 0
		shard.StartBlockID = start.ID     // this MAY be block 1 or 2, instead of expected 0
		shard.EndBlockID = end.ID
		shard.EndBlockTime = end.Time
		if shard.EndBlock != end.Num {
			return nil, fmt.Errorf("invalid end block assertion on shard")
		}
	}

	return shard, nil
}

type filePathFunc func(baseBlockNum uint64, suffix string) string

type ShardIndex struct {
	index.Index

	IndexBuilder    index.IndexBuilder
	IndexTargetPath string

	// These two values represent the "potential" start and end
	// block. It doesn't mean there is actual data within those two
	// blocks: ex: if block endBlock had 0 transactions, we wouldn't
	// shrink `endBlock`.
	//
	// The chain of [startBlock, endBlock] -> [startBlock, endBlock]
	// *must* be absolutely continuous from index to index within the
	// process, and between the different segments of indexes
	// (readOnly, merging, writable, and live)
	StartBlock     uint64 // inclusive
	StartBlockID   string
	StartBlockTime time.Time
	EndBlock       uint64 // inclusive
	EndBlockID     string
	EndBlockTime   time.Time
	analysisQueue  *index.AnalysisQueue

	blockID string // for live indexes // TODO is this stil needed here in archive/?

	mergeDone bool

	writableIndexFilePathFunc filePathFunc

	Lock sync.RWMutex
}

type BoundaryBlockInfo struct {
	Num  uint64
	ID   string
	Time time.Time
}

func (s *ShardIndex) GetBoundaryBlocks(idx index.Index) (start *BoundaryBlockInfo, end *BoundaryBlockInfo, err error) {
	reader, err := idx.Reader()
	if err != nil {
		return nil, nil, fmt.Errorf("getting reader: %s", err)
	}
	defer reader.Close()

	q, err := query.ParseQuery([]byte(`{"prefix": "meta:boundary", "field": "_id"}`))
	if err != nil {
		return nil, nil, fmt.Errorf("parsing our query: %s", err)
	}
	searcher, err := q.Searcher(reader, nil, bsearch.SearcherOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("running searcher: %s", err)
	}
	defer searcher.Close()

	//sortOrder := bsearch.ParseSortOrderStrings([]string{"ins_block"})
	coll := collector.NewTopNCollector(1000000, 0, nil)

	if err = coll.Collect(context.Background(), searcher, reader); err != nil {
		return nil, nil, fmt.Errorf("collecting query: %s", err)
	}

	start = &BoundaryBlockInfo{}
	end = &BoundaryBlockInfo{}
	results := coll.Results()
	for _, el := range results {
		zlog.Info("boundary", zap.String("b", el.ID))
		parts := strings.Split(el.ID, ":")
		if len(parts) < 4 {
			return nil, nil, fmt.Errorf("cannot get boundary blocks, invalid parts")
		}
		switch parts[2] {
		case "start_num":
			val, err := strconv.ParseUint(parts[3], 10, 32)
			if err != nil {
				return nil, nil, fmt.Errorf("parse start num: %s", err)
			}
			start.Num = val
		case "end_num":
			val, err := strconv.ParseUint(parts[3], 10, 32)
			if err != nil {
				return nil, nil, fmt.Errorf("parse end num: %s", err)
			}
			end.Num = val
		case "end_id":
			end.ID = parts[3]
		case "start_id":
			start.ID = parts[3]
		case "end_time":
			val, err := time.Parse(TimeFormatBleveID, parts[3])
			if err != nil {
				return nil, nil, fmt.Errorf("parse end time")
			}
			end.Time = val
		case "start_time":
			val, err := time.Parse(TimeFormatBleveID, parts[3])
			if err != nil {
				return nil, nil, fmt.Errorf("parse start time")
			}
			start.Time = val
		}
	}

	return
}

func (s *ShardIndex) containsBlockNum(blockNum uint64) bool {
	return blockNum >= s.StartBlock && blockNum <= s.EndBlock
}

func (s *ShardIndex) WritablePath(suffix string) string {
	return s.writableIndexFilePathFunc(s.StartBlock, suffix)
}

func (s *ShardIndex) RequestCoversFullRange(low, high uint64) bool {
	if low > s.StartBlock {
		return false
	}
	if high < s.EndBlock {
		return false
	}
	return true
}

func (s *ShardIndex) requestEndBlockPassed(sortDesc bool, endBlock uint64) bool {
	if sortDesc {
		// EXCLUSIVE endBlock > INCLUSIVE s.endBlock
		return endBlock > s.EndBlock
	}
	// EXCLUSIVE end block <= INCLUSIVE s.startBlock
	return endBlock <= s.StartBlock
}

func (s *ShardIndex) Close() error {
	if s.Index != nil {
		// TODO fix me: use zlog of caller (indexer, archive)
		zlog.Debug("close is called on shardIndex", zap.Uint64("start_block", s.StartBlock))
		if s.analysisQueue != nil {
			s.analysisQueue.Close()
			s.analysisQueue = nil
		}

		return s.Index.Close()
	} else {
		return nil
	}
}

func isValidBoundaries(start, end *BoundaryBlockInfo) bool {
	return isValidStartBoundary(start) && isValidEndBoundary(end)
}

func isValidStartBoundary(start *BoundaryBlockInfo) bool {
	if start == nil {
		return false
	}
	if start.ID == "" || start.Time.IsZero() {
		return false
	}
	return true
}

func isValidEndBoundary(end *BoundaryBlockInfo) bool {
	if end == nil {
		return false
	}
	if end.ID == "" || end.Num == 0 || end.Time.IsZero() {
		return false
	}
	return true
}
