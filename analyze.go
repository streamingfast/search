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
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/index/scorch"
	bsearch "github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collector"
	"github.com/blevesearch/bleve/search/query"
	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

func CheckIndexIntegrity(path string, shardSize uint64) error {
	var errs []error
	addError := func(err error) MultiError {
		errs = append(errs, err)
		return errs
	}

	idx, err := scorch.NewScorch("eos", map[string]interface{}{
		"read_only": true,
		"path":      path,
	}, nil)
	if err != nil {
		return addError(fmt.Errorf("new scorch: %s", err))
	}

	if err = idx.Open(); err != nil {
		return addError(fmt.Errorf("open index: %s", err))
	}
	defer idx.Close()

	reader, err := idx.Reader()
	if err != nil {
		return addError(fmt.Errorf("getting reader: %s", err))
	}
	defer reader.Close()

	q, err := query.ParseQuery([]byte(`{"prefix": "meta:blknum:", "field": "_id"}`))
	if err != nil {
		return addError(fmt.Errorf("parsing our query: %s", err))
	}

	searcher, err := q.Searcher(reader, nil, bsearch.SearcherOptions{})
	if err != nil {
		return addError(fmt.Errorf("running searcher: %s", err))
	}
	defer searcher.Close()

	//sortOrder := bsearch.ParseSortOrderStrings([]string{"ins_block"})
	coll := collector.NewTopNCollector(1000000, 0, nil)

	if err = coll.Collect(context.Background(), searcher, reader); err != nil {
		return addError(fmt.Errorf("collecting query: %s", err))
	}

	lowest := uint64(math.MaxUint64)
	highest := uint64(0)

	elements := map[uint64]bool{}
	blks := make([]int, len(coll.Results()))
	for i, el := range coll.Results() {
		blk := strings.Split(el.ID, ":")[2]
		val, err := strconv.ParseUint(blk, 10, 32)
		if err != nil {
			return addError(fmt.Errorf("parse uint: %s", err))
		}

		if val < lowest {
			lowest = val
		}
		if val > highest {
			highest = val
		}
		elements[val] = true
		blks[i] = int(val)
		//fmt.Println(" -", idx, el.ID, val)
	}

	sort.Ints(blks)

	if uint64(len(coll.Results())) != shardSize || (highest-lowest) != uint64(shardSize-1) {

		if lowest == bstream.GetProtocolFirstBlock && (highest-lowest) == uint64(shardSize-bstream.GetProtocolFirstBlock) {
			zlog.Debug("integrity check assuming protocol on first shard, passed", zap.Uint64("protocol_first_block", bstream.GetProtocolFirstBlock))
		} else {
			errs = addError(fmt.Errorf("integrity check failed, expected %d results, actual %d, lowest: %d, highest: %d, path: %s", shardSize, len(coll.Results()), lowest, highest, path))
		}
	}
	for i := 0; i < len(blks)-1; i++ {
		prev := blks[i]
		next := blks[i+1]
		if prev != next-1 {
			errs = addError(fmt.Errorf("discontinuity within index, prev: %d, next: %d, path: %s", prev, next, path))
		}
	}

	// Done like that to avoid problematic nil check between `error` and `MultiError` interface(s)
	if len(errs) > 0 {
		return MultiError(errs)
	}

	return nil
}

type MultiError []error

func (m MultiError) Error() string {
	var out []string
	for idx, err := range m {
		out = append(out, fmt.Sprintf("%d) %s", idx+1, err.Error()))
	}

	return strings.Join(out, ", ")
}
