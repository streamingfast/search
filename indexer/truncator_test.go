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

	"github.com/dfuse-io/bstream"
	"github.com/stretchr/testify/assert"
)

func TestIndexer_shouldTruncate(t *testing.T) {
	tests := []struct {
		Name             string
		headBlock        *bstream.Block
		blockCount       uint64
		expectedBlockNum uint64
	}{
		{
			Name:             "no head block",
			headBlock:        nil,
			blockCount:       3000,
			expectedBlockNum: 0,
		},
		{
			Name:             "head block below block count",
			headBlock:        &bstream.Block{Number: 10},
			blockCount:       3000,
			expectedBlockNum: 0,
		},
		{
			Name:             "head block equal block count",
			headBlock:        &bstream.Block{Number: 3000},
			blockCount:       3000,
			expectedBlockNum: 0,
		},
		{
			Name:             "head block greater then block count",
			headBlock:        &bstream.Block{Number: 5500},
			blockCount:       3000,
			expectedBlockNum: 2500,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			truncate := &Truncator{
				indexer: &Indexer{
					libBlock: test.headBlock,
				},
				blockCount: test.blockCount,
			}
			assert.Equal(t, test.expectedBlockNum, truncate.targetIndex())
		})
	}
}
