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
)

// v0: 0:blocknum:(irr|headBlockID):trxPrefix
func NewCursor(blockNum uint64, headBlockID string, trxPrefix string) string {
	if len(trxPrefix) > 12 {
		trxPrefix = trxPrefix[:12]
	}
	return fmt.Sprintf("1:%d:%s:%s", blockNum, headBlockID, trxPrefix)
}
