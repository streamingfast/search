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
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type QueryMetrics struct {
	zlog *zap.Logger

	// The raw query string
	query string

	// The actual query range used to log the low/high block num
	lowBlockNum  uint64
	highBlockNum uint64

	descending bool

	// The request start time
	startTime time.Time

	// The actual request total time
	requestDuration time.Duration

	// The amount of time elapsed between start of request and instant where we received the first transaction result
	timeToFirstResult time.Duration

	// The number of transactions we have seen from our shard results
	TransactionSeenCount int64

	// Actual maximum potential indexes that might be searched fully (high block num - low block num / shardSize usually)
	potentialIndexesCount uint64

	// The number of actual walked indexes, regardless if it was actually used to return results or not (cancel index)
	SearchedIndexesCount *atomic.Uint32

	// The number of actual usueful searched indexes, those that were used to return results
	UtilizedIndexesCount *atomic.Uint32

	// The total transaction count searched through searched indexes
	searchedTrxCount *atomic.Uint32

	// The total transaction count utilized through utilized indexes
	UtilizedTrxCount *atomic.Uint32

	// The total time passed cumulatively in each searched indexes
	searchedTotalDuration *atomic.Duration

	// The total time passed cumulatively in each utilized indexes
	UtilizedTotalDuration *atomic.Duration
}

func NewQueryMetrics(zlog *zap.Logger, descending bool, query string, shardSize uint64, low, high uint64) *QueryMetrics {
	potentialIndexesCount := (high-low)/shardSize + 1
	return &QueryMetrics{
		zlog:                  zlog,
		query:                 query,
		descending:            descending,
		lowBlockNum:           low,
		highBlockNum:          high,
		startTime:             time.Now(),
		potentialIndexesCount: potentialIndexesCount,
		SearchedIndexesCount:  atomic.NewUint32(0),
		UtilizedIndexesCount:  atomic.NewUint32(0),
		searchedTrxCount:      atomic.NewUint32(0),
		UtilizedTrxCount:      atomic.NewUint32(0),
		searchedTotalDuration: atomic.NewDuration(0),
		UtilizedTotalDuration: atomic.NewDuration(0),
	}
}

func (m *QueryMetrics) MarkFirstResult() {
	// If we already marked first result, skip all subsequent calls
	if m.timeToFirstResult != 0 {
		return
	}

	m.timeToFirstResult = time.Since(m.startTime)
}

func (m *QueryMetrics) Finalize() {
	m.requestDuration = time.Since(m.startTime)

	// This is a big hack.
	//
	// We need to actually wait for all pending go routines for this search
	// query to complete. However, we do not have any easy programmatic ways to
	// wait for all searched but not utilized indexes which are being cancelled and
	// should close really shortly.
	//
	// As such, simply by waiting 1 second, we let the time for all the wasted
	// indexes to report their duration to the metrics object. It's a best effort
	// to ensure we have a fully populated query metrics.
	go func() {
		time.Sleep(1 * time.Second)
		m.log()
	}()
}

func (m *QueryMetrics) log() {
	m.zlog.Info("query metrics",
		zap.String("query", m.query),
		zap.Duration("duration", m.requestDuration),
		zap.Bool("descending", m.descending),
		zap.Uint64("low_block_num", m.lowBlockNum),
		zap.Uint64("high_block_num", m.highBlockNum),
		zap.Duration("time_to_first_result", m.timeToFirstResult),
		zap.Int64("transaction_seen_count", m.TransactionSeenCount),
		zap.Uint64("potential_indexes_count", m.potentialIndexesCount),
		zap.Uint32("searched_indexes_count", m.SearchedIndexesCount.Load()),
		zap.Uint32("utilized_indexes_count", m.UtilizedIndexesCount.Load()),
		zap.Uint32("searched_trx_count", m.searchedTrxCount.Load()),
		zap.Uint32("utilized_trx_count", m.UtilizedTrxCount.Load()),
		zap.Duration("searched_total_duration", m.searchedTotalDuration.Load()),
		zap.Duration("utilized_total_duration", m.UtilizedTotalDuration.Load()),
	)
}
