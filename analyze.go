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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch"
	bsearch "github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collector"
	"github.com/blevesearch/bleve/search/query"
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

func CheckIndexIntegrity(path string, shardSize uint64) (*indexMetaInfo, error) {
	idx, err := scorch.NewScorch("data", map[string]interface{}{
		"forceSegmentType":    "zap",
		"forceSegmentVersion": 14,
		"read_only":           true,
		"path":                path,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("new scorch: %s", err)
	}

	if err = idx.Open(); err != nil {
		return nil, fmt.Errorf("open index: %s", err)
	}
	defer idx.Close()

	reader, err := idx.Reader()
	if err != nil {
		return nil, fmt.Errorf("getting reader: %s", err)
	}
	defer reader.Close()

	metaInfo := &indexMetaInfo{
		HighestBlockNum: uint64(0),
		LowestBlockNum:  uint64(math.MaxUint64),
	}

	coll, err := getCollection(reader, "meta:blknum:")
	if err != nil {
		return nil, fmt.Errorf("getting meta block number meta ids: %w", err)
	}

	err = metaInfo.setBlockMetaInfo(coll)
	if err != nil {
		return metaInfo, fmt.Errorf("error parse block meta data: %w", err)
	}

	coll, err = getCollection(reader, "meta:boundary")
	if err != nil {
		return nil, fmt.Errorf("getting meta boundaries meta ids: %w", err)
	}

	err = metaInfo.setBoundaryMetaInfo(coll)
	if err != nil {
		return metaInfo, fmt.Errorf("error parse block meta data: %w", err)
	}

	errs := metaInfo.Validate(shardSize, path)

	// Done like that to avoid problematic nil check between `error` and `MultiError` interface(s)
	if len(errs) > 0 {
		return metaInfo, MultiError(errs)
	}

	return metaInfo, nil
}

type MultiError []error

func (m MultiError) Error() string {
	var out []string
	for idx, err := range m {
		out = append(out, fmt.Sprintf("%d) %s", idx+1, err.Error()))
	}

	return strings.Join(out, ", ")
}

type indexMetaInfo struct {
	StartBlock      *BoundaryBlockInfo
	EndBlock        *BoundaryBlockInfo
	LowestBlockNum  uint64
	HighestBlockNum uint64
	OrderedBlockNum []int
}

func (i *indexMetaInfo) Validate(expectedShardSize uint64, path string) MultiError {
	var errs []error
	addError := func(err error) MultiError {
		errs = append(errs, err)
		return errs
	}

	// Checking Block Num ordering and containing the full range
	if uint64(len(i.OrderedBlockNum)) != expectedShardSize || (i.HighestBlockNum-i.LowestBlockNum) != expectedShardSize-1 {
		if i.LowestBlockNum == bstream.GetProtocolFirstStreamableBlock && (i.HighestBlockNum-i.LowestBlockNum) == expectedShardSize-bstream.GetProtocolFirstStreamableBlock-1 {
			zlog.Debug("integrity check assuming protocol on first shard, passed",
				zap.Uint64("protocol_first_block", bstream.GetProtocolFirstStreamableBlock),
			)
		} else {
			//integrity check failed, expected 25 results, actual 23, lowest: 2, highest: 24, protocol's lowest block: 1, path: /Users/cbillett/t/eth-data/dfuse-data/search/indexer/0000000000.bleve, 2) boundary check failed: missing start block boundary"}
			errs = addError(fmt.Errorf("integrity check failed, expected %d results, actual %d, lowest: %d, highest: %d, protocol's lowest block: %d, path: %s", expectedShardSize, len(i.OrderedBlockNum), i.LowestBlockNum, i.HighestBlockNum, bstream.GetProtocolFirstStreamableBlock, path))
		}
	}
	for j := 0; j < len(i.OrderedBlockNum)-1; j++ {
		prev := i.OrderedBlockNum[j]
		next := i.OrderedBlockNum[j+1]
		if prev != next-1 {
			errs = addError(fmt.Errorf("discontinuity within index, prev: %d, next: %d, path: %s", prev, next, path))
		}
	}

	if i.StartBlock == nil {
		errs = addError(fmt.Errorf("boundary check failed: missing start block boundary"))
	} else if !isValidStartBoundary(i.StartBlock) {
		errs = addError(fmt.Errorf("boundary check failed: invalid start block boundary: start_block_id: %d start_block_time: %s, start_block_num: %s", i.StartBlock.Num, i.StartBlock.ID, i.StartBlock.Time.Format("2006-01-02 15:04:05 -0700")))
	}

	if i.EndBlock == nil {
		errs = addError(fmt.Errorf("boundary check failed: missing end block boundary"))
	} else if !isValidStartBoundary(i.EndBlock) {
		errs = addError(fmt.Errorf("boundary check failed: invalid end block boundary: end_block_id: %d end_block_time: %s, end_block_num: %s", i.EndBlock.Num, i.EndBlock.ID, i.EndBlock.Time.Format("2006-01-02 15:04:05 -0700")))
	}

	return errs
}

func (i *indexMetaInfo) storeBlockNum(value uint64) {
	if value < i.LowestBlockNum {
		i.LowestBlockNum = value
	}

	if value > i.HighestBlockNum {
		i.HighestBlockNum = value
	}
}

type blockInfo struct {
	index    int
	blockNum int
}

func (m *indexMetaInfo) setBoundaryMetaInfo(coll *collector.TopNCollector) error {
	for _, el := range coll.Results() {
		chunks := strings.Split(el.ID, ":")
		boundaryType := chunks[2]
		boundaryValue := chunks[3]
		switch boundaryType {
		case "start_num":
			val, err := strconv.ParseUint(boundaryValue, 10, 32)
			if err != nil {
				return fmt.Errorf("parse uint: %s", err)
			}
			if m.StartBlock == nil {
				m.StartBlock = &BoundaryBlockInfo{}
			}
			m.StartBlock.Num = val
		case "end_num":
			val, err := strconv.ParseUint(boundaryValue, 10, 32)
			if err != nil {
				return fmt.Errorf("parse uint: %s", err)
			}
			if m.EndBlock == nil {
				m.EndBlock = &BoundaryBlockInfo{}
			}
			m.EndBlock.Num = val
		case "end_id":
			if m.EndBlock == nil {
				m.EndBlock = &BoundaryBlockInfo{}
			}
			m.EndBlock.ID = boundaryValue
		case "start_id":
			if m.StartBlock == nil {
				m.StartBlock = &BoundaryBlockInfo{}
			}
			m.StartBlock.ID = boundaryValue
		case "end_time":
			val, err := time.Parse(TimeFormatBleveID, boundaryValue)
			if err != nil {
				return fmt.Errorf("parse uint: %s", err)
			}
			if m.EndBlock == nil {
				m.EndBlock = &BoundaryBlockInfo{}
			}
			m.EndBlock.Time = val
		case "start_time":
			val, err := time.Parse(TimeFormatBleveID, boundaryValue)
			if err != nil {
				return fmt.Errorf("parse uint: %s", err)
			}
			if m.StartBlock == nil {
				m.StartBlock = &BoundaryBlockInfo{}
			}
			m.StartBlock.Time = val
		}
	}
	return nil
}

func (m *indexMetaInfo) setBlockMetaInfo(coll *collector.TopNCollector) error {
	m.OrderedBlockNum = make([]int, len(coll.Results()))
	for i, el := range coll.Results() {
		chunks := strings.Split(el.ID, ":")
		metaValue := chunks[2]
		val, err := strconv.ParseUint(metaValue, 10, 32)
		if err != nil {
			return fmt.Errorf("parse uint: %s", err)
		}
		m.storeBlockNum(val)
		m.OrderedBlockNum[i] = int(val)
	}

	sort.Ints(m.OrderedBlockNum)
	return nil
}

func getCollection(reader index.IndexReader, pattern string) (*collector.TopNCollector, error) {
	q, err := query.ParseQuery([]byte(fmt.Sprintf(`{"prefix": "%s", "field": "_id"}`, pattern)))
	if err != nil {
		return nil, fmt.Errorf("parsing our query: %s", err)
	}

	searcher, err := q.Searcher(reader, nil, bsearch.SearcherOptions{})
	if err != nil {
		return nil, fmt.Errorf("running searcher: %s", err)
	}
	defer searcher.Close()

	coll := collector.NewTopNCollector(1000000, 0, nil)

	if err = coll.Collect(context.Background(), searcher, reader); err != nil {
		return nil, fmt.Errorf("collecting query: %s", err)
	}
	return coll, nil
}
