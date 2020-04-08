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
	"time"

	"github.com/dfuse-io/bstream"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"go.uber.org/zap"
)

// GetIrreversibleBlock will do whatever it takes to fetch the
// irreversible block at height `blockNum`, up to a number of retries. -1 retries mean forever
func GetIrreversibleBlock(blockmetaCli pbblockmeta.BlockIDClient, blockNum uint64, ctx context.Context, retries int) (bstream.BlockRef, error) {
	if blockNum == 0 {
		return bstream.NewBlockRef("0000000000000000000000000000000000000000000000000000000000000000", 0), nil
	}

	started := time.Now()
	tried := 0
	for {
		idResp, err := blockmetaCli.NumToID(ctx, &pbblockmeta.NumToIDRequest{BlockNum: blockNum})
		if err == nil && idResp.Irreversible {
			return bstream.NewBlockRef(idResp.Id, blockNum), nil
		}

		tried++
		if retries != -1 && tried > retries {
			zlog.Info("could not get irreversible block after retrying", zap.Int("retries", retries), zap.Uint64("blocknum", blockNum), zap.Error(err))
			return nil, fmt.Errorf("cannot retrieve irreversible block")
		}
		if tried%30 == 0 {
			zlog.Warn("still trying to fetch irreversible block from blockmeta", zap.Duration("since", time.Since(started)), zap.Error(err), zap.Uint64("block_num", blockNum), zap.Int("retries", retries))
		}

		time.Sleep(time.Second)
	}
}

func GetLibBlock(blockmetaCli pbblockmeta.BlockIDClient) (bstream.BlockRef, error) {
	return nil, nil
}
