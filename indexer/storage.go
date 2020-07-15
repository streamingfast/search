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

func (i *Indexer) NextUnindexedBlockPast(startBlockNum uint64) (nextStartBlockNum uint64) {
	nextStartBlockNum = startBlockNum

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	remote, err := i.indexesStore.ListFiles(ctx, fmt.Sprintf("shards-%d/", i.shardSize), ".tmp", 9999999)
	if err != nil {
		zlog.Error("listing files from indexes store", zap.Error(err))
		return
	}

	remotePathRE := regexp.MustCompile(`(\d{10})\.bleve\.tar\.zst`)

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

func (i *Indexer) NextUnindexedBlockPast(startBlockNum uint64) (nextStartBlockNum uint64) {
	nextStartBlockNum = startBlockNum

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	remote, err := i.indexesStore.ListFiles(ctx, fmt.Sprintf("shards-%d/", i.shardSize), ".tmp", 9999999)
	if err != nil {
		zlog.Error("listing files from indexes store", zap.Error(err))
		return
	}

	remotePathRE := regexp.MustCompile(`(\d{10})\.bleve\.tar\.zst`)

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

func (i *Indexer) alignStartBlock(startBlock uint64) uint64 {
	return startBlock - (startBlock % i.shardSize)
}
