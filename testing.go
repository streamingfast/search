package search

import (
	"context"
	"sort"
	"strings"

	bsearch "github.com/blevesearch/bleve/search"
	"github.com/streamingfast/bstream"
	v1 "github.com/streamingfast/pbgo/dfuse/search/v1"
	"go.uber.org/zap"
)

type testSearchMatch struct {
	blockNumber   uint64   `json:"blk"`    // Current block for this trx
	trxIDPrefix   string   `json:"prefix"` // ID prefix
	actionIndexes []uint16 `json:"acts"`   // Action indexes within the transactions
	index         uint64   `json:"idx"`    // Index of the matching transaction within a block (depends on order of sort)
}

func (t *testSearchMatch) BlockNum() uint64 {
	return t.blockNumber
}

func (t *testSearchMatch) TransactionIDPrefix() string {
	return t.trxIDPrefix
}

func (t *testSearchMatch) GetIndex() uint64 {
	return t.index
}

func (t *testSearchMatch) SetIndex(index uint64) {
	t.index = index
}

func (t *testSearchMatch) FillProtoSpecific(match *v1.SearchMatch, blk *bstream.Block) error {
	return nil
}

type testTrxResult struct {
	id       string
	blockNum uint64
}

var TestMatchCollector = func(ctx context.Context, lowBlockNum, highBlockNum uint64, results bsearch.DocumentMatchCollection) (out []SearchMatch, err error) {
	trxs := make(map[string][]uint16)
	var trxList []*testTrxResult

	for _, el := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		blockNum, trxID, actionIdx, skip := testExplodeDocumentID(el.ID)
		if skip {
			continue
		}

		if blockNum < lowBlockNum || blockNum > highBlockNum {
			continue
		}

		if _, found := trxs[trxID]; !found {
			trxList = append(trxList, &testTrxResult{
				id:       trxID,
				blockNum: blockNum,
			})
		}

		trxs[trxID] = append(trxs[trxID], actionIdx)
	}

	for _, trx := range trxList {
		actions := trxs[trx.id]
		sort.Slice(actions, func(i, j int) bool { return actions[i] < actions[j] })

		out = append(out, &testSearchMatch{
			blockNumber:   trx.blockNum,
			trxIDPrefix:   trx.id,
			actionIndexes: actions,
		})
	}

	return out, nil
}

func testExplodeDocumentID(ref string) (blockNum uint64, trxID string, actionIdx uint16, skip bool) {
	var err error
	chunks := strings.Split(ref, ":")
	chunksCount := len(chunks)
	if chunksCount != 3 || chunks[0] == "meta" { // meta, flatten, etc.
		skip = true
		return
	}

	blockNum32, err := fromHexUint32(chunks[0])
	if err != nil {
		zlog.Panic("woah, block num invalid?", zap.Error(err))
	}

	blockNum = uint64(blockNum32)

	trxID = chunks[1]
	actionIdx, err = fromHexUint16(chunks[2])
	if err != nil {
		zlog.Panic("woah, action index invalid?", zap.Error(err))
	}

	return
}
