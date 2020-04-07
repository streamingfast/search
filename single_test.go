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
	"testing"

	_ "github.com/dfuse-io/bstream/codecs/deos"
	"github.com/stretchr/testify/assert"
)

func init() {
	// TODO: move all these tests to the `dfuse-eosio:/search` repo/path
	InitEOSIndexedFields()
}

func TestSortIndexMatches(t *testing.T) {
	tests := []struct {
		name    string
		matches []SearchMatch
		expect  []SearchMatch
	}{
		{
			name: "multiple ascending",
			matches: []SearchMatch{
				&EOSSearchMatch{BlockNumber: 10, TrxIDPrefix: "a"},
				&EOSSearchMatch{BlockNumber: 10, TrxIDPrefix: "b"},
				&EOSSearchMatch{BlockNumber: 10, TrxIDPrefix: "c"},
				&EOSSearchMatch{BlockNumber: 11, TrxIDPrefix: "d"},
				&EOSSearchMatch{BlockNumber: 11, TrxIDPrefix: "e"},
				&EOSSearchMatch{BlockNumber: 12, TrxIDPrefix: "f"},
				&EOSSearchMatch{BlockNumber: 13, TrxIDPrefix: "g"},
				&EOSSearchMatch{BlockNumber: 14, TrxIDPrefix: "h"},
				&EOSSearchMatch{BlockNumber: 14, TrxIDPrefix: "i"},
				&EOSSearchMatch{BlockNumber: 14, TrxIDPrefix: "j"},
				&EOSSearchMatch{BlockNumber: 14, TrxIDPrefix: "k"},
			},
			expect: []SearchMatch{
				&EOSSearchMatch{BlockNumber: 10, Index: 0, TrxIDPrefix: "a"},
				&EOSSearchMatch{BlockNumber: 10, Index: 1, TrxIDPrefix: "b"},
				&EOSSearchMatch{BlockNumber: 10, Index: 2, TrxIDPrefix: "c"},
				&EOSSearchMatch{BlockNumber: 11, Index: 0, TrxIDPrefix: "d"},
				&EOSSearchMatch{BlockNumber: 11, Index: 1, TrxIDPrefix: "e"},
				&EOSSearchMatch{BlockNumber: 12, Index: 0, TrxIDPrefix: "f"},
				&EOSSearchMatch{BlockNumber: 13, Index: 0, TrxIDPrefix: "g"},
				&EOSSearchMatch{BlockNumber: 14, Index: 0, TrxIDPrefix: "h"},
				&EOSSearchMatch{BlockNumber: 14, Index: 1, TrxIDPrefix: "i"},
				&EOSSearchMatch{BlockNumber: 14, Index: 2, TrxIDPrefix: "j"},
				&EOSSearchMatch{BlockNumber: 14, Index: 3, TrxIDPrefix: "k"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := adjustMatchesIndex(test.matches)
			assert.Equal(t, test.expect, res)
		})
	}
}
