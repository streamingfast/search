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

package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexer_alignStartBlock(t *testing.T) {
	tests := []struct {
		name             string
		shardSize        uint64
		startBlock       uint64
		expectStartBlock uint64
	}{
		{
			name:             "0 start block",
			startBlock:       0,
			shardSize:        50,
			expectStartBlock: 0,
		},
		{
			name:             "aligned start block",
			startBlock:       16898050,
			shardSize:        50,
			expectStartBlock: 16898050,
		},
		{
			name:             "mis-aligned start block",
			startBlock:       18901242,
			shardSize:        50,
			expectStartBlock: 18901200,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexer := &Indexer{shardSize: test.shardSize}
			assert.Equal(t, test.expectStartBlock, indexer.alignStartBlock(test.startBlock))
		})
	}
}
