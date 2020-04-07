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

	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

// GetIrreversibleBlock will do whatever it takes to fetch the
// irreversible block at height `blockNum`, up to a number of retries. -1 retries mean forever
func GetIrreversibleBlock(blockmetaCli pbblockmeta.BlockIDClient, blockNum uint64, ctx context.Context, retries int) (bstream.BlockRef, error) {
	if blockNum == 0 {
		return bstream.NewBlockRef("0000000000000000000000000000000000000000000000000000000000000000", 0), nil
	}

	tried := 0
	for {
		idResp, err := blockmetaCli.NumToID(ctx, &pbblockmeta.NumToIDRequest{BlockNum: blockNum})
		if err != nil {
			return nil, err
		}

		if idResp.Irreversible {
			return bstream.NewBlockRef(idResp.Id, blockNum), nil
		}

		tried++
		if retries != -1 && tried > retries {
			zlog.Info("could not get irreversible block after retrying", zap.Int("retries", retries), zap.Uint64("blocknum", blockNum))
			return nil, fmt.Errorf("cannot retrieve irreversible block")
		}
		time.Sleep(time.Second)
	}
}

func GetLibBlock(blockmetaCli pbblockmeta.BlockIDClient) (bstream.BlockRef, error) {
	return nil, nil
}
