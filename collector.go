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
	"sort"

	bsearch "github.com/blevesearch/bleve/search"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
)

type MatchCollector interface {
	Collect(ctx context.Context, lowBlockNum, highBlockNum uint64, results bsearch.DocumentMatchCollection) ([]SearchMatch, error)
}

var MatchCollectorByType = map[pbbstream.Protocol]MatchCollector{
	pbbstream.Protocol_EOS: &EOSMatchCollector{},
	//	pbbstream.Protocol_ETH: &ETHMatchCollector{},
}

type EOSMatchCollector struct{}

type trxResult struct {
	id       string
	blockNum uint64
}

func (c *EOSMatchCollector) Collect(ctx context.Context, lowBlockNum, highBlockNum uint64, results bsearch.DocumentMatchCollection) (out []SearchMatch, err error) {
	trxs := make(map[string][]uint16)
	var trxList []*trxResult

	for _, el := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		blockNum, trxID, actionIdx, skip := ExplodeEOSDocumentID(el.ID)
		if skip {
			continue
		}

		if blockNum < lowBlockNum || blockNum > highBlockNum {
			continue
		}

		if _, found := trxs[trxID]; !found {
			trxList = append(trxList, &trxResult{
				id:       trxID,
				blockNum: blockNum,
			})
		}

		trxs[trxID] = append(trxs[trxID], actionIdx)
	}

	for _, trx := range trxList {
		actions := trxs[trx.id]
		sort.Slice(actions, func(i, j int) bool { return actions[i] < actions[j] })

		out = append(out, &EOSSearchMatch{
			TrxIDPrefix:   trx.id,
			ActionIndexes: actions,
			BlockNumber:   trx.blockNum,
		})
	}

	return out, nil
}
