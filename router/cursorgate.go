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
	"strings"
)

type cursorGate struct {
	c            *cursor
	cursorPassed bool
	descending   bool
}

func NewCursorGate(c *cursor, descending bool) *cursorGate {
	return &cursorGate{
		c:            c,
		descending:   descending,
		cursorPassed: (c == nil),
	}
}

func (g *cursorGate) passed(blockNum uint64, trxIDPrefix string) bool {
	if g.cursorPassed {
		return true
	}

	if g.c.trxPrefix == "" {
		if g.descending && (blockNum < g.c.blockNum) {
			g.cursorPassed = true
			return true
		}
		if !g.descending && (blockNum > g.c.blockNum) {
			g.cursorPassed = true
			return true
		}
		return false
	}

	if (g.c.blockNum == blockNum) && strings.HasPrefix(trxIDPrefix, g.c.trxPrefix) {
		g.cursorPassed = true
		return false
	}

	return false
}

func (g *cursorGate) hasGate() bool {
	return !((g.c.blockNum == 0) && (g.c.trxPrefix == ""))
}
