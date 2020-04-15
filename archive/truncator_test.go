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

	"github.com/stretchr/testify/assert"
)

func TestIndexPool_shouldTruncate(t *testing.T) {
	tests := []struct {
		Name                    string
		lowestServeableBlockNum uint64
		targetBlockNum          uint64
		expectedValue           bool
	}{
		{
			Name:                    "empty index pool",
			lowestServeableBlockNum: 0,
			targetBlockNum:          0,
			expectedValue:           false,
		},
		{
			Name:                    "on init with preset index pool",
			lowestServeableBlockNum: 101112650,
			targetBlockNum:          0,
			expectedValue:           false,
		},
		{
			Name:                    "empty index pool",
			lowestServeableBlockNum: 50,
			targetBlockNum:          50,
			expectedValue:           false,
		},
		{
			Name:                    "empty index pool",
			lowestServeableBlockNum: 50,
			targetBlockNum:          100,
			expectedValue:           true,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			truncate := &Truncator{
				indexPool: &IndexPool{
					LowestServeableBlockNum: test.lowestServeableBlockNum,
				},
				targetTruncateBlock: test.targetBlockNum,
			}
			assert.Equal(t, test.expectedValue, truncate.shouldTruncate())
		})
	}
}
