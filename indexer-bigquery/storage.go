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
	"context"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"os"
	"regexp"
	"sync"
	"time"

	"go.uber.org/zap"
)

type filePathFunc func(baseBlockNum uint64, suffix string) string

type BigQueryShardIndex struct {
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

	mergeDone bool

	writableIndexFilePathFunc filePathFunc

	Lock sync.RWMutex

	ocfFile *os.File
	ocfWriter *goavro.OCFWriter
}

func NewBigQueryShardIndex(baseBlockNum uint64, shardSize uint64, pathFunc filePathFunc) (*BigQueryShardIndex, error) {
	si, err := newShardIndex(baseBlockNum, shardSize, pathFunc)
	if err != nil {
		return nil, err
	}
	return si, nil
}

func newShardIndex(baseBlockNum uint64, shardSize uint64, pathFunc filePathFunc) (*BigQueryShardIndex, error) {
	shard := &BigQueryShardIndex{
		StartBlock:                baseBlockNum,
		EndBlock:                  baseBlockNum + shardSize - 1,
		writableIndexFilePathFunc: pathFunc,
	}
	if baseBlockNum == 0 && shardSize == 0 {
		shard.EndBlock = 0
		return shard, nil
	}

	return shard, nil
}

func (s *BigQueryShardIndex) containsBlockNum(blockNum uint64) bool {
	return blockNum >= s.StartBlock && blockNum <= s.EndBlock
}

func (s *BigQueryShardIndex) WritablePath(suffix string) string {
	return s.writableIndexFilePathFunc(s.StartBlock, suffix)
}

func (s *BigQueryShardIndex) Index(doc map[string]interface{}) error {
	return nil
}

func (s *BigQueryShardIndex) Close() error {
	//TODO: Close AVRO cleanly
	return nil
}

/////

func (i *IndexerBigQuery) NextBaseBlockAfter(startBlockNum uint64) (nextStartBlockNum uint64) {
	nextStartBlockNum = startBlockNum

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	remote, err := i.indexesStore.ListFiles(ctx, fmt.Sprintf("bigquery-shards-%d/", i.shardSize), ".tmp", 9999999)
	if err != nil {
		zlog.Error("listing files from indexes store", zap.Error(err))
		return
	}

	remotePathRE := regexp.MustCompile(`(\d{10})\.avro`)

	count := 0
	for _, file := range remote {
		match := remotePathRE.FindStringSubmatch(file)
		if match == nil {
			zlog.Info("Skipping non-index file in remote storage", zap.String("file", file))
			continue
		}

		fileStartBlock := startBlockFromFileName(match[1])
		if fileStartBlock <= startBlockNum {
			count++
			if count%1000 == 0 {
				zlog.Info("skipping index file before start block 1/1000",
					zap.String("file", file),
					zap.Int("skipped_file_count", count),
					zap.Uint64("start_block", startBlockNum),
				)
			}
			continue
		}

		if fileStartBlock > nextStartBlockNum+i.shardSize {
			zlog.Info("found a hole to fill, starting here",
				zap.Uint64("file_start_block", fileStartBlock),
				zap.Uint64("expected_start_block", (nextStartBlockNum+i.shardSize)))
			break
		}
		nextStartBlockNum = fileStartBlock
	}

	return i.alignStartBlock(nextStartBlockNum)
}

func (i *IndexerBigQuery) alignStartBlock(startBlock uint64) uint64 {
	return startBlock - (startBlock % i.shardSize)
}