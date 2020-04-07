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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainsBlockNum(t *testing.T) {
	tests := []struct {
		indexStart uint64
		indexEnd   uint64
		blockNum   uint64
		expect     bool
	}{
		{
			20, 29,
			20,
			true,
		},
		{
			20, 29,
			25,
			true,
		},
		{
			20, 29,
			29,
			true,
		},
		{
			20, 29,
			30,
			false,
		},
		{
			20, 29,
			19,
			false,
		},
		{
			20, 20,
			20,
			true,
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			index := &ShardIndex{StartBlock: test.indexStart, EndBlock: test.indexEnd}
			res := index.containsBlockNum(test.blockNum)
			assert.Equal(t, test.expect, res)
		})
	}
}

func TestEndBlockPast(t *testing.T) {
	tests := []struct {
		desc        bool
		startBlock  uint64
		endBlock    uint64
		reqEndBlock uint64
		expect      bool
	}{
		{
			false,
			20, 29,
			20, // <- EXCLUSIVE endBlock, this is a pain
			true,
		},
		{
			false,
			20, 29,
			21,
			false,
		},
		{
			false,
			20, 29,
			25,
			false,
		},
		{
			false,
			20, 29,
			29,
			false,
		},
		{
			false,
			20, 29,
			30,
			false,
		},
		{
			false,
			20, 29,
			19,
			true,
		},
		{
			true,
			20, 29,
			31,
			true,
		},
		{
			true,
			20, 29,
			29,
			false,
		},
		{
			true,
			20, 29,
			20,
			false,
		},
		{
			true,
			20, 29,
			19,
			false,
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			index := &ShardIndex{StartBlock: test.startBlock, EndBlock: test.endBlock}
			res := index.requestEndBlockPassed(test.desc, test.reqEndBlock)
			assert.Equal(t, test.expect, res)
		})
	}
}
