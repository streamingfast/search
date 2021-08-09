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

func TestIterator(t *testing.T) {
	tests := []struct {
		name        string
		current     uint64
		readPool    []*search.ShardIndex
		expectStart uint64
		expectNil   bool
		endBlock    uint64 //request end block
		sortDesc    bool
	}{
		{
			name:     "within read pool",
			current:  20,
			endBlock: 1000,
			readPool: []*search.ShardIndex{
				{StartBlock: 10},
				{StartBlock: 20},
			},
			expectStart: 20,
		},
		{
			name:     "bounded in progress",
			current:  10,
			endBlock: 17,
			readPool: []*search.ShardIndex{
				{StartBlock: 0, EndBlock: 9},
				{StartBlock: 10, EndBlock: 19},
				{StartBlock: 20, EndBlock: 29},
			},
			expectStart: 10,
		},
		{
			name:     "bounded in progress DESC",
			sortDesc: true,
			current:  19,
			endBlock: 17,
			readPool: []*search.ShardIndex{
				{StartBlock: 0, EndBlock: 9},
				{StartBlock: 10, EndBlock: 19},
				{StartBlock: 20, EndBlock: 29},
			},
			expectStart: 10,
		},
		{
			name:     "bounded finished DESC",
			sortDesc: true,
			current:  9,
			endBlock: 17,
			readPool: []*search.ShardIndex{
				{StartBlock: 0, EndBlock: 9},
				{StartBlock: 10, EndBlock: 19},
				{StartBlock: 20, EndBlock: 29},
			},
			expectNil: true,
		},
		{
			name:     "bounded finished",
			current:  20,
			endBlock: 17,
			readPool: []*search.ShardIndex{
				{StartBlock: 10, EndBlock: 19},
				{StartBlock: 20, EndBlock: 29},
			},
			expectNil: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := &IndexPool{
				ReadPool:  test.readPool,
				ShardSize: 10,
			}

			it := &indexIterator{
				pool:               pool,
				currentBlock:       test.current,
				readPoolStartBlock: test.readPool[0].StartBlock,
				readPoolSnapshot:   test.readPool,
				endBlock:           test.endBlock,
				sortDesc:           test.sortDesc,
			}
			idx, _, release := it.Next()
			if test.expectNil {
				assert.Nil(t, idx)
			} else {
				assert.Equal(t, test.expectStart, idx.StartBlock)
			}
			release()
		})
	}
}

func TestGetIterator(t *testing.T) {
	p := &IndexPool{
		LowestServeableBlockNum: 10000,
	}
	_, err := p.GetIndexIterator(5000, 0, false)
	assert.Error(t, err)
}
