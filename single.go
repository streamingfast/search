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
	"os"
	"time"

	"github.com/blevesearch/bleve/index"
	bsearch "github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collector"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

const MaxInt = int(^uint(0) >> 1)

var sortOrderAsc = bsearch.ParseSortOrderStrings([]string{"block_num", "trx_idx"})
var sortOrderDesc = bsearch.ParseSortOrderStrings([]string{"-block_num", "-trx_idx"})

func RunSingleIndexQuery(
	ctx context.Context,
	sortDesc bool,
	lowBlockNum, highBlockNum uint64,
	matchCollector MatchCollector,
	bquery *BleveQuery,
	index index.Index,
	releaseIndex func(),
	metrics *QueryMetrics,
) (
	out []SearchMatch,
	err error,
) {
	defer releaseIndex()

	if metrics != nil {
		// The searched duration **must** be done here. The reason is that this is the single
		// location where the full search time is available without disruption with actual
		// useful consumption of the results or not.
		startTime := time.Now()
		defer func() {
			metrics.searchedTotalDuration.Add(time.Since(startTime))
			metrics.searchedTrxCount.Add(uint32(len(out)))
		}()
	}

	reader, err := index.Reader()
	if err != nil {
		return nil, fmt.Errorf("getting reader: %s", err)
	}
	defer reader.Close()

	searcher, err := bquery.BleveQuery().Searcher(reader, nil, bsearch.SearcherOptions{})
	if err != nil {
		return nil, fmt.Errorf("running searcher: %s", err)
	}
	defer searcher.Close()

	var sortOrder bsearch.SortOrder
	if sortDesc {
		sortOrder = sortOrderDesc.Copy()
	} else {
		sortOrder = sortOrderAsc.Copy()
	}

	coll := collector.NewTopNCollector(MaxInt, 0, sortOrder)
	err = coll.Collect(ctx, searcher, reader)
	if err != nil {
		return nil, err
	}

	if os.Getenv("DEBUG_SEARCH") != "" {
		zlogger := logging.Logger(ctx, zlog)
		zlogger.Debug("subquery returned", zap.Uint64("docs", coll.Total()), zap.Duration("timing", coll.Took()))
	}

	allSingleIndexResults := coll.Results()

	matches, err := matchCollector(ctx, lowBlockNum, highBlockNum, allSingleIndexResults)
	if err != nil {
		return nil, err
	}

	matches = adjustMatchesIndex(matches)

	return matches, nil
}

func adjustMatchesIndex(matches []SearchMatch) []SearchMatch {
	var idx uint64
	var blockNum uint64
	for _, match := range matches {
		if blockNum == match.BlockNum() {
			idx++
		} else {
			idx = 0
			blockNum = match.BlockNum()
		}

		match.SetIndex(idx)
	}

	return matches
}
