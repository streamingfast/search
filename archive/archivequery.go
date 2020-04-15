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
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/logging"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/metrics"
	"go.opencensus.io/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// archiveQuery is responsible for going through the archives, and
// streaming out results to `streaming`

type archiveQuery struct {
	parentCtx       context.Context
	maxQueryThreads int
	pool            *IndexPool
	matchCollector  search.MatchCollector

	sortDesc                  bool
	lowBlockNum, highBlockNum uint64
	bquery                    *search.BleveQuery

	Results       chan search.SearchMatch
	Errors        chan error
	LastBlockRead *atomic.Uint64

	metrics *search.QueryMetrics
	zlog    *zap.Logger
}

func (b *ArchiveBackend) newArchiveQuery(
	ctx context.Context,
	sortDesc bool,
	lowBlockNum, highBlockNum uint64,
	bquery *search.BleveQuery,
	metrics *search.QueryMetrics,
) *archiveQuery {
	return &archiveQuery{
		parentCtx: ctx,

		pool:            b.Pool,
		maxQueryThreads: b.MaxQueryThreads,
		matchCollector:  b.matchCollector,

		LastBlockRead: &atomic.Uint64{},
		Results:       make(chan search.SearchMatch, 10),
		Errors:        make(chan error, 2),

		bquery:       bquery,
		sortDesc:     sortDesc,
		lowBlockNum:  lowBlockNum,
		highBlockNum: highBlockNum,

		metrics: metrics,
		zlog:    logging.Logger(ctx, zlog),
	}
}

func (q *archiveQuery) checkBoundaries(availableLow, availableHigh uint64) error {
	if q.sortDesc {
		if q.highBlockNum > availableHigh {
			return fmt.Errorf("high block num requested (%d) higher than highest available (%d)", q.highBlockNum, availableHigh)
		}
	} else {
		if q.lowBlockNum < availableLow {
			return fmt.Errorf("requested lower boundary (%d) lower than lowest available (%d)", q.lowBlockNum, availableLow)
		}
	}

	return nil
}

func (q *archiveQuery) run() {
	ctx, cancel := context.WithCancel(q.parentCtx)
	defer cancel()

	if q.maxQueryThreads == 0 {
		panic("max query threads can't be zero")
	}

	ctx, span := startSpan(ctx, "running archive query", trace.StringAttribute("query", q.bquery.Raw))
	defer span.End()

	q.zlog.Info("run archive query", zap.Any("bquery", q.bquery), zap.Uint64("low_block_num", q.lowBlockNum), zap.Uint64("high_block_num", q.highBlockNum))

	// Note: SOMETHING needs to be written in this pipe, for the `linearizeStreamResults` to
	// function properly.
	incomingPerShardResults := make(chan *incomingResult, q.maxQueryThreads)

	indexIterator, err := q.pool.GetIndexIterator(q.lowBlockNum, q.highBlockNum, q.sortDesc)
	if err != nil {
		q.Errors <- err
		return
	}

	if q.pool.emptyResultsCache != nil {
		indexIterator.LoadRoaring(q.bquery.Raw)
		defer indexIterator.OptimizeAndPublishRoaring()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.linearizeStreamResults(ctx, incomingPerShardResults)
	}()

	eg := llerrgroup.New(q.maxQueryThreads)

	qto := NewQueryThreadsOptimizer(q.maxQueryThreads, q.sortDesc, q.lowBlockNum, eg)

IndexLoop:
	for {
		qto.Optimize()
		if eg.Stop() {
			break
		}

		select {
		case <-ctx.Done():
			// upstream is already gone, so we don't care about the error message
			// returned to them..
			break IndexLoop
		default:
		}

		index, skipIndex, releaseIndex := indexIterator.Next()
		if index == nil {
			q.zlog.Debug("reached last index for query", zap.Uint64("base", indexIterator.CurrentBase()))
			break
		}

		effectiveEndBlock := index.EndBlock
		effectiveStartBlock := index.StartBlock
		if q.sortDesc && q.lowBlockNum > index.StartBlock {
			effectiveStartBlock = q.lowBlockNum
		}
		if !q.sortDesc && q.highBlockNum < index.EndBlock {
			effectiveEndBlock = q.highBlockNum
		}

		shardResult := &incomingResult{
			resultChan:      make(chan *singleIndexResult, 1),
			indexStartBlock: effectiveStartBlock,
			indexEndBlock:   effectiveEndBlock,
		}

		// TODO: ensure we ONLY MARK empty when we're sure the FULL RANGE has been
		// read

		if skipIndex {
			incomingPerShardResults <- shardResult
			shardResult.resultChan <- &singleIndexResult{}
			eg.Free() // since we called eg.Stop() earlier and won't be calling eg.Go()
			continue
		}

		metrics.IndexesScanned.Inc()
		metrics.ActiveOpenedIndexCount.Inc()
		statsAwareIndexReleaser := func() {
			releaseIndex()
			metrics.ActiveOpenedIndexCount.Dec()
		}

		// If we don't put those two together, we risk dead locking on
		// `incomingPerShardResults <- res`, in case
		// `linearizeStreamResults` quits on `ctx.Done()` before it
		// reads in the next `res`.
		select {
		case <-ctx.Done():
			statsAwareIndexReleaser()
			break IndexLoop
		case incomingPerShardResults <- shardResult:
		}

		eg.Go(func() error {
			if q.metrics != nil {
				q.metrics.SearchedIndexesCount.Inc()
			}

			startTime := time.Now()
			matches, err := search.RunSingleIndexQuery(ctx, q.sortDesc, q.lowBlockNum, q.highBlockNum, q.matchCollector, q.bquery, index, statsAwareIndexReleaser, q.metrics)
			if err != nil {
				return err
			}

			qto.ReportShard(int(index.StartBlock), len(matches))

			if len(matches) == 0 && index.RequestCoversFullRange(q.lowBlockNum, q.highBlockNum) {
				zlog.Debug("marking empty", zap.Uint64("start_bock", index.StartBlock))
				indexIterator.MarkEmpty(index.StartBlock)
			}

			shardResult.resultChan <- &singleIndexResult{
				Matches: matches,

				// The duration here is not pushed directly in the `queryMetrics` object
				// because at this exact point, we do not know yet if the duration should be
				// added to the utilized bucket or not yet. As such, we do not want to add
				// straight to the query metrics object. Instead, we defer the decision later
				// to the entity that is doing the consumption of this shard result. See
				// shard result consumption comments to better grasp why it's like that.
				duration: time.Since(startTime),
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		// if err is NotFound or somethin'..
		q.Errors <- err
		return
	}

	close(incomingPerShardResults)

	wg.Wait()
}

func (q *archiveQuery) linearizeStreamResults(ctx context.Context, incomingPerShardResults chan *incomingResult) {
	defer close(q.Results)

	for {
		select {
		case <-ctx.Done():
			return
		case nextShardResults := <-incomingPerShardResults:
			if nextShardResults == nil {
				q.zlog.Info("run query: all shards processed", zap.Uint64("last_read", q.LastBlockRead.Load()))
				return
			}
			select {
			case <-ctx.Done():
				return
			case result := <-nextShardResults.resultChan:
				if q.metrics != nil {
					// We will actually utilized at least one transaction from this
					// shard results, which will contain all the matches for a single
					// shard. So, we assume that a visited shard, even for a single
					// or for all transactions is a utilized index shard.

					q.metrics.UtilizedIndexesCount.Inc()
					q.metrics.UtilizedTotalDuration.Add(result.duration)
					q.metrics.UtilizedTrxCount.Add(uint32(len(result.Matches)))
				}

				if !q.sortDesc {
					q.LastBlockRead.Store(nextShardResults.indexEndBlock)
				} else {
					q.LastBlockRead.Store(nextShardResults.indexStartBlock)
				}

				for _, match := range result.Matches {
					select {
					case q.Results <- match:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

type singleIndexResult struct {
	Matches  []search.SearchMatch
	duration time.Duration
}
