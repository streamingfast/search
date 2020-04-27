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
	"regexp"
	"time"

	"go.uber.org/zap"
)

type Truncator struct {
	indexer    *Indexer
	blockCount uint64
}

const ShardsTruncationLeeway = 5

func NewTruncator(indexer *Indexer, blockCount uint64) *Truncator {
	return &Truncator{
		indexer:    indexer,
		blockCount: blockCount,
	}
}

func (t *Truncator) Launch() {
	zlog.Info("launching index truncator")
	for {
		time.Sleep(30 * time.Second)
		err := t.attemptTruncation()
		if err != nil {
			zlog.Warn("error truncate index files", zap.Error(err))
		}
	}
}

func (t *Truncator) attemptTruncation() error {
	targetBlockNum := t.targetIndex()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	remote, err := t.indexer.indexesStore.ListFiles(ctx, fmt.Sprintf("shards-%d/", t.indexer.shardSize), ".tmp", 9999999)
	if err != nil {
		return fmt.Errorf("listing files from indexes store: %s", err)
	}

	zlog.Info("truncation attempt",
		zap.Uint64("target_block_num", targetBlockNum),
		zap.Int("remote_file_count", len(remote)),
		zap.Uint64("block_count", t.blockCount),
		zap.Uint64("shardsize_leeway_blk_count", (ShardsTruncationLeeway*t.indexer.shardSize)))

	remotePathRE := regexp.MustCompile(`(\d{10})\.bleve\.tar\.zst`)
	for _, filePath := range remote {
		match := remotePathRE.FindStringSubmatch(filePath)
		if match == nil {
			zlog.Info("Skipping non-index file in remote storage", zap.String("file", filePath))
			continue
		}

		fileStartBlock := startBlockFromFileName(match[1])

		if fileStartBlock < targetBlockNum {
			zlog.Info("truncating gstore index file",
				zap.Uint64("block_num", fileStartBlock),
				zap.String("index_filepath", filePath),
				zap.Uint64("target_block_num", targetBlockNum))
			err = t.indexer.indexesStore.DeleteObject(ctx, filePath)
			if err != nil {
				return fmt.Errorf("error deleting gstore index file %d - %q", fileStartBlock, filePath)
			}
		}
	}
	return nil
}

func (t *Truncator) targetIndex() uint64 {
	if t.indexer.libBlock != nil {
		if t.indexer.libBlock.Number > t.blockCount {
			return t.indexer.libBlock.Number - t.blockCount - (ShardsTruncationLeeway * t.indexer.shardSize)
		}
		return 0
	}
	return 0
}
