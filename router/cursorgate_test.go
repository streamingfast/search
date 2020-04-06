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

package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCursorPasses(t *testing.T) {
	tests := []struct {
		name         string
		cursorGate   *cursorGate
		cursorPassed bool
		trxBlock     uint64
		trxIndex     string
		expectPass   bool
	}{
		// No cursor, forward
		{
			name: "fwd, no cursor, with block only",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  0,
				trxPrefix: "",
			}, false),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: true,
		},
		{
			name:       "fwd, no cursor, with block only adn trx prefix",
			cursorGate: NewCursorGate(nil, false),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: true,
		},
		// No cursor, backwards
		{
			name:       "back, no cursor, with block only",
			cursorGate: NewCursorGate(nil, true),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: true,
		},
		{
			name:       "back, no cursor, with block only and trx prefix",
			cursorGate: NewCursorGate(nil, true),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: true,
		},
		//
		// With cursor
		// with forward cursor, only with a block requirement
		{
			name: "fwd, with cursor, not passed cursor yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "",
			}, false),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: false,
		},
		{
			name: "fwd, with  cursor, on block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "",
			}, false),
			trxBlock:   10,
			trxIndex:   "trx5",
			expectPass: false,
		},
		{
			name: "fwd, with  cursor, after block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "",
			}, false),
			cursorPassed: true,
			trxBlock:     15,
			trxIndex:     "trx8",
			expectPass:   true,
		},
		{
			name: "fwd, with  cursor, seen on prior block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "",
			}, false),
			cursorPassed: true,
			trxBlock:     20,
			trxIndex:     "trx11",
			expectPass:   true,
		},
		// with backward cursor, only with a block requirement
		{
			name: "back, with cursor, not passed cursor yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "",
			}, true),
			trxBlock:   20,
			trxIndex:   "trx11",
			expectPass: false,
		},
		{
			name: "back, with  cursor, on block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "",
			}, true),
			trxBlock:   15,
			trxIndex:   "trx8",
			expectPass: false,
		},
		{
			name: "back, with  cursor, after block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "",
			}, true),
			trxBlock:   10,
			trxIndex:   "trx5",
			expectPass: true,
		},
		{
			name: "back, with  cursor, passed on prior block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "",
			}, false),
			cursorPassed: true,
			trxBlock:     5,
			trxIndex:     "trx2",
			expectPass:   true,
		},
		// with forward cursor, with  block & trx prefix requirement
		{
			name: "fwd, with cursor, not passed cursor yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			trxBlock:   5,
			trxIndex:   "trx2",
			expectPass: false,
		},
		{
			name: "fwd, with  cursor, on block , with trx not seen yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			trxBlock:   10,
			trxIndex:   "trx4",
			expectPass: false,
		},
		{
			name: "fwd, with  cursor, on block and on trx",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			trxBlock:   10,
			trxIndex:   "trx5",
			expectPass: false,
		},
		{
			name: "fwd, with  cursor, on block and trx seen",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			cursorPassed: true,
			trxBlock:     10,
			trxIndex:     "trx6",
			expectPass:   true,
		},
		{
			name: "fwd, with  cursor, after block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			cursorPassed: true,
			trxBlock:     15,
			trxIndex:     "trx8",
			expectPass:   true,
		},
		{
			name: "fwd, with  cursor seen on prior block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  10,
				trxPrefix: "trx5",
			}, false),
			cursorPassed: true,
			trxBlock:     20,
			trxIndex:     "trx11",
			expectPass:   true,
		},
		// with backward cursor, with  block & trx prefix requirement
		{
			name: "back, with cursor, not passed cursor yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			trxBlock:   20,
			trxIndex:   "trx10",
			expectPass: false,
		},
		{
			name: "back, with  cursor, on block , with trx not seen yet",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			trxBlock:   15,
			trxIndex:   "trx9",
			expectPass: false,
		},
		{
			name: "back, with  cursor, on block and on trx",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			trxBlock:   15,
			trxIndex:   "trx8",
			expectPass: false,
		},
		{
			name: "back, with  cursor, on block and trx seen",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			cursorPassed: true,
			trxBlock:     15,
			trxIndex:     "trx7",
			expectPass:   true,
		},
		{
			name: "back, with  cursor, after block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			cursorPassed: true,
			trxBlock:     10,
			trxIndex:     "trx5",
			expectPass:   true,
		},
		{
			name: "back, with  cursor seen on prior block",
			cursorGate: NewCursorGate(&cursor{
				blockNum:  15,
				trxPrefix: "trx8",
			}, true),
			cursorPassed: true,
			trxBlock:     5,
			trxIndex:     "trx2",
			expectPass:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.cursorPassed {
				test.cursorGate.cursorPassed = test.cursorPassed
			}

			passed := test.cursorGate.passed(test.trxBlock, test.trxIndex)

			assert.Equal(t, test.expectPass, passed)
		})
	}
}
