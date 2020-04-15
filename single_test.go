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

	"github.com/stretchr/testify/assert"
)

func TestSortIndexMatches(t *testing.T) {
	tests := []struct {
		name    string
		matches []SearchMatch
		expect  []SearchMatch
	}{
		{
			name: "multiple ascending",
			matches: []SearchMatch{
				&testSearchMatch{blockNumber: 10, trxIDPrefix: "a"},
				&testSearchMatch{blockNumber: 10, trxIDPrefix: "b"},
				&testSearchMatch{blockNumber: 10, trxIDPrefix: "c"},
				&testSearchMatch{blockNumber: 11, trxIDPrefix: "d"},
				&testSearchMatch{blockNumber: 11, trxIDPrefix: "e"},
				&testSearchMatch{blockNumber: 12, trxIDPrefix: "f"},
				&testSearchMatch{blockNumber: 13, trxIDPrefix: "g"},
				&testSearchMatch{blockNumber: 14, trxIDPrefix: "h"},
				&testSearchMatch{blockNumber: 14, trxIDPrefix: "i"},
				&testSearchMatch{blockNumber: 14, trxIDPrefix: "j"},
				&testSearchMatch{blockNumber: 14, trxIDPrefix: "k"},
			},
			expect: []SearchMatch{
				&testSearchMatch{blockNumber: 10, index: 0, trxIDPrefix: "a"},
				&testSearchMatch{blockNumber: 10, index: 1, trxIDPrefix: "b"},
				&testSearchMatch{blockNumber: 10, index: 2, trxIDPrefix: "c"},
				&testSearchMatch{blockNumber: 11, index: 0, trxIDPrefix: "d"},
				&testSearchMatch{blockNumber: 11, index: 1, trxIDPrefix: "e"},
				&testSearchMatch{blockNumber: 12, index: 0, trxIDPrefix: "f"},
				&testSearchMatch{blockNumber: 13, index: 0, trxIDPrefix: "g"},
				&testSearchMatch{blockNumber: 14, index: 0, trxIDPrefix: "h"},
				&testSearchMatch{blockNumber: 14, index: 1, trxIDPrefix: "i"},
				&testSearchMatch{blockNumber: 14, index: 2, trxIDPrefix: "j"},
				&testSearchMatch{blockNumber: 14, index: 3, trxIDPrefix: "k"},
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
