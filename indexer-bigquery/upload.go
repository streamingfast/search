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
	"go.uber.org/zap"
	"strconv"
)

func startBlockFromFileName(filename string) uint64 {
	startBlock, _ := strconv.ParseInt(filename, 10, 64)
	return uint64(startBlock)
}

func (p *Pipeline) Upload(baseIndex uint64, indexPath string) (err error) {
	destinationPath := fmt.Sprintf("bigquery-shards-%d/%010d.avro", p.shardSize, baseIndex)
	zlog.Info("upload: index", zap.Uint64("base", baseIndex), zap.String("destination_path", destinationPath))

	ctx := context.Background()
	err = p.indexesStore.PushLocalFile(ctx, indexPath, destinationPath)

	return
}