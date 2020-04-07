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

var metrics = dmetrics.NewSet()

// Archive
var ActiveOpenedIndexCount = metrics.NewGauge("active_opened_index_count")
var RoarCacheHitIndexesSkipped = metrics.NewCounter("total_indexes_skipped_because_of_roar_cache", "Number of indexes that were skipped because they were informed by roaring bitmaps that a given query yields no results in that index")
var IndexesScanned = metrics.NewCounter("total_indexes_scanned", "Number of indexes that were scanned in a distinct query")

var RoarCacheMiss = metrics.NewCounter("roar_cache_misses", "Number of roar cache miss")
var RoarCacheHit = metrics.NewCounter("roar_cache_hits", "Number of roar cache hits")
var RoarCacheFail = metrics.NewCounter("roar_cache_failures", "Number of roar cache lookup failures")

// Indexer
var CatchUpBlocksPerSecond = metrics.NewGauge("catch_up_blocks_per_second")
var CatchUpDocsPerSecond = metrics.NewGauge("catch_up_docs_per_second")
var LastWrittenBlockNumber = metrics.NewGauge("last_written_block_number", "%s (should be LIB)")

// Router
var DeletedPeerCount = metrics.NewCounter("deleted_peer_count")
var InflightRequestCount = metrics.NewGauge("inflight_request_count")
var ErrorRequestCount = metrics.NewCounter("error_request_count")
var TotalRequestCount = metrics.NewCounter("total_request_count")
var ErrorBackendCount = metrics.NewCounter("error_backend_count")
var FullContiguousBlockRange = metrics.NewGauge("full_contiguous_block_range")
var IRRBlockNumber = metrics.NewGauge("irr_block_number", "Current %s (from Dmesh)")

// Common
var ActiveQueryCount = metrics.NewGauge("active_query_count")
var TailBlockNumber = metrics.NewGauge("tail_block_number", "Current %s (from Dmesh)")

func ServeMetrics() {
	dmetrics.Serve(":9102")
}

func init() {
	metrics.Register()
}
