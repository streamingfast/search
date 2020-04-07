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
	"encoding/json"
	"strings"

	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	pbsearch "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/bstream"
)

type SearchMatch interface {
	BlockNum() uint64
	TransactionIDPrefix() string

	GetIndex() uint64
	SetIndex(index uint64)

	FillProtoSpecific(match *pbsearch.SearchMatch, blk *bstream.Block) error
}

// EOS

type EOSSearchMatch struct {
	TrxIDPrefix   string   `json:"prefix"` // ID prefix
	ActionIndexes []uint16 `json:"acts"`   // Action indexes within the transactions
	BlockNumber   uint64   `json:"blk"`    // Current block for this trx
	Index         uint64   `json:"idx"`    // Index of the matching transaction within a block (depends on order of sort)
}

func (m *EOSSearchMatch) BlockNum() uint64 {
	return m.BlockNumber
}

func (m *EOSSearchMatch) GetIndex() uint64 {
	return m.Index
}

func (m *EOSSearchMatch) TransactionIDPrefix() string {
	return m.TrxIDPrefix
}

func (m *EOSSearchMatch) SetIndex(index uint64) {
	m.Index = index
}

func (m *EOSSearchMatch) FillProtoSpecific(match *pbsearch.SearchMatch, block *bstream.Block) error {
	eosMatch := &pbsearch.EOSMatch{}
	match.Specific = &pbsearch.SearchMatch_Eos{
		Eos: eosMatch,
	}

	if block != nil {
		eosMatch.Block = m.buildBlockTrxPayload(block)
		if m.TrxIDPrefix == "" {
			return nil
		}
	}

	eosMatch.ActionIndexes = Uint16to32s(m.ActionIndexes)

	return nil
}

func (m *EOSSearchMatch) buildBlockTrxPayload(block *bstream.Block) *pbsearch.EOSBlockTrxPayload {
	blk := block.ToNative().(*pbdeos.Block)

	if m.TrxIDPrefix == "" {
		return &pbsearch.EOSBlockTrxPayload{
			BlockHeader: blk.Header,
			BlockID:     blk.ID(),
		}
	}

	for _, trx := range blk.TransactionTraces {
		fullTrxID := trx.Id
		if !strings.HasPrefix(fullTrxID, m.TrxIDPrefix) {
			continue
		}

		out := &pbsearch.EOSBlockTrxPayload{}
		out.BlockHeader = blk.Header
		out.BlockID = blk.Id
		out.Trace = trx
		return out
	}

	// FIXME (MATT): Is this even possible?
	return nil
}

func mustToJSON(v interface{}) []byte {
	if v == nil {
		return nil
	}
	cnt, _ := json.Marshal(v)
	return cnt
}
