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
	"context"
	"fmt"

	"github.com/abourget/llerrgroup"
	bsearch "github.com/blevesearch/bleve/search"
	"github.com/dfuse-io/search"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func (b *ArchiveBackend) WarmUpArchive(ctx context.Context, low, high uint64, bquery *search.BleveQuery) error {
	if b.MaxQueryThreads == 0 {
		panic("max query threads can't be zero")
	}
	counter := atomic.NewUint64(0)
	eg := llerrgroup.New(b.MaxQueryThreads)
	indexIterator, err := b.Pool.GetIndexIterator(low, high, false)
	if err != nil {
		return err
	}

	for {
		if eg.Stop() {
			break
		}

		index, _, releaseIndex := indexIterator.Next()
		if index == nil {
			break
		}

		eg.Go(func() error {

			defer releaseIndex()
			reader, err := index.Reader()
			if err != nil {
				return fmt.Errorf("getting reader: %s", err)
			}
			defer reader.Close()

			searcher, err := bquery.BleveQuery().Searcher(reader, nil, bsearch.SearcherOptions{})
			if err != nil {
				return fmt.Errorf("running searcher: %s", err)
			}
			defer searcher.Close()

			count := searcher.Count()
			zlog.Debug("searcher count in index", zap.Uint64("start_block", index.StartBlock), zap.Uint64("count", count))
			counter.Add(count)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	zlog.Info("warmup query returned results", zap.Uint64("counter", counter.Load()), zap.String("query", bquery.Raw))
	return nil
}
