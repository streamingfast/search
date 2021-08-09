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

package archive

import (
	"testing"

	"github.com/streamingfast/search"
	"github.com/stretchr/testify/assert"
)

func Test_nextReadOnlyIndexBlock(t *testing.T) {
	tests := []struct {
		name               string
		shardSize          uint64
		readPool           []*search.ShardIndex
		expectedStartBlock uint64
	}{
		{
			name:               "empty read pool",
			shardSize:          10,
			readPool:           []*search.ShardIndex{},
			expectedStartBlock: 0,
		},
		{
			name:      "read pool with one index index",
			shardSize: 10,
			readPool: []*search.ShardIndex{
				{
					StartBlock: 10,
				},
			},
			expectedStartBlock: 20,
		},
		{
			name:      "read pool with mutliple index",
			shardSize: 500,
			readPool: []*search.ShardIndex{
				{StartBlock: 0},
				{StartBlock: 500},
				{StartBlock: 1000},
			},
			expectedStartBlock: 1500,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := &IndexPool{ReadPool: test.readPool, ShardSize: test.shardSize}
			assert.Equal(t, test.expectedStartBlock, pool.nextReadOnlyIndexBlock())
		})
	}
}

func TestIndexPool_getLowestThresholdBlock(t *testing.T) {
	tests := []struct {
		Name          string
		readPool      []*search.ShardIndex
		blockNum      uint64
		expectedValue uint64
	}{
		{
			Name:          "empty index pool",
			blockNum:      149,
			readPool:      []*search.ShardIndex{},
			expectedValue: 0,
		},
		{
			Name:     "index pool with block info",
			blockNum: 149,
			readPool: []*search.ShardIndex{
				{StartBlock: 0, EndBlock: 49},
				{StartBlock: 50, EndBlock: 99},
				{StartBlock: 100, EndBlock: 149},
				{StartBlock: 150, EndBlock: 299},
				{StartBlock: 200, EndBlock: 249},
				{StartBlock: 250, EndBlock: 299},
			},
			expectedValue: 150,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {

			lowestServeableBlockNum := uint64(0)
			if len(test.readPool) > 0 {
				lowestServeableBlockNum = test.readPool[0].StartBlock
			}
			indexPool := &IndexPool{
				LowestServeableBlockNum: lowestServeableBlockNum,
				ReadPool:                test.readPool,
			}
			assert.Equal(t, test.expectedValue, indexPool.LowestServeableBlockNumAbove(test.blockNum))
		})
	}
}
