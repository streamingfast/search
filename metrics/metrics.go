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
)

var MetricSet = dmetrics.NewSet()

// Archive
var ActiveOpenedIndexCount = MetricSet.NewGauge("active_opened_index_count")
var RoarCacheHitIndexesSkipped = MetricSet.NewCounter("total_indexes_skipped_because_of_roar_cache", "Number of indexes that were skipped because they were informed by roaring bitmaps that a given query yields no results in that index")
var IndexesScanned = MetricSet.NewCounter("total_indexes_scanned", "Number of indexes that were scanned in a distinct query")

var RoarCacheMiss = MetricSet.NewCounter("roar_cache_misses", "Number of roar cache miss")
var RoarCacheHit = MetricSet.NewCounter("roar_cache_hits", "Number of roar cache hits")
var RoarCacheFail = MetricSet.NewCounter("roar_cache_failures", "Number of roar cache lookup failures")

// Indexer
var CatchUpBlocksPerSecond = MetricSet.NewGauge("catch_up_blocks_per_second")
var CatchUpDocsPerSecond = MetricSet.NewGauge("catch_up_docs_per_second")
var LastWrittenBlockNumber = MetricSet.NewGauge("last_written_block_number", "%s (should be LIB)")

// Router
var DeletedPeerCount = MetricSet.NewCounter("deleted_peer_count")
var InflightRequestCount = MetricSet.NewGauge("inflight_request_count")
var ErrorRequestCount = MetricSet.NewCounter("error_request_count")
var TotalRequestCount = MetricSet.NewCounter("total_request_count")
var ErrorBackendCount = MetricSet.NewCounter("error_backend_count")
var FullContiguousBlockRange = MetricSet.NewGauge("full_contiguous_block_range")
var IRRBlockNumber = MetricSet.NewGauge("irr_block_number", "Current %s (from Dmesh)")

// Common
var ActiveQueryCount = MetricSet.NewGauge("active_query_count")
var TailBlockNumber = MetricSet.NewGauge("tail_block_number", "Current %s (from Dmesh)")
