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

package metrics

import (
	"github.com/dfuse-io/dmetrics"
	"sync"
)

var mutex sync.Mutex

func Register(sets ...*dmetrics.Set) {
	mutex.Lock()
	defer mutex.Unlock()

	// Common set will be registered only once (soft gate downtstream), lazily on first Run() passed mutex
	dmetrics.Register(CommonMetricSet)
	// App-specific metrics
	dmetrics.Register(sets...)
}

// Common
var CommonMetricSet = dmetrics.NewSet()
var ActiveQueryCount = CommonMetricSet.NewGauge("active_query_count")
var TailBlockNumber = CommonMetricSet.NewGauge("tail_block_number", "Current %s (from Dmesh)")

// LiveResolver
var LiveMetricSet = dmetrics.NewSet()

// ForkResolver
var ForkResolverMetricSet = dmetrics.NewSet()

// Archive
var ArchiveMetricsSet = dmetrics.NewSet()
var ActiveOpenedIndexCount = ArchiveMetricsSet.NewGauge("active_opened_index_count")
var RoarCacheHitIndexesSkipped = ArchiveMetricsSet.NewCounter("total_indexes_skipped_because_of_roar_cache", "Number of indexes that were skipped because they were informed by roaring bitmaps that a given query yields no results in that index")
var IndexesScanned = ArchiveMetricsSet.NewCounter("total_indexes_scanned", "Number of indexes that were scanned in a distinct query")
var RoarCacheMiss = ArchiveMetricsSet.NewCounter("roar_cache_misses", "Number of roar cache miss")
var RoarCacheHit = ArchiveMetricsSet.NewCounter("roar_cache_hits", "Number of roar cache hits")
var RoarCacheFail = ArchiveMetricsSet.NewCounter("roar_cache_failures", "Number of roar cache lookup failures")

// Indexer
var IndexerMetricSet = dmetrics.NewSet()
var CatchUpBlocksPerSecond = IndexerMetricSet.NewGauge("catch_up_blocks_per_second")
var CatchUpDocsPerSecond = IndexerMetricSet.NewGauge("catch_up_docs_per_second")
var LastWrittenBlockNumber = IndexerMetricSet.NewGauge("last_written_block_number", "%s (should be LIB)")

// Router
var RouterMetricSet = dmetrics.NewSet()
var InflightRequestCount = RouterMetricSet.NewGauge("inflight_request_count")
var ErrorRequestCount = RouterMetricSet.NewCounter("error_request_count")
var TotalRequestCount = RouterMetricSet.NewCounter("total_request_count")
var ErrorBackendCount = RouterMetricSet.NewCounter("error_backend_count")
var FullContiguousBlockRange = RouterMetricSet.NewGauge("full_contiguous_block_range")
var IRRBlockNumber = RouterMetricSet.NewGauge("irr_block_number", "Current %s (from Dmesh)")
