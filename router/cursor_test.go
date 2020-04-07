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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewCursorFromString(t *testing.T) {
	tests := []struct {
		name           string
		cursor         string
		expectedCursor *cursor
		expectedError  error
	}{
		{
			name:   "valid live cursor",
			cursor: "1:10:abc124:trx2",
			expectedCursor: &cursor{
				blockNum:    10,
				headBlockID: "abc124",
				trxPrefix:   "trx2",
			},
		},
		{
			name:   "valid archive cursor",
			cursor: "1:4::trx1",
			expectedCursor: &cursor{
				blockNum:    4,
				headBlockID: "",
				trxPrefix:   "trx1",
			},
		},
		{
			name:          "invalid cursor v0",
			cursor:        "lib:4:",
			expectedError: fmt.Errorf("invalid cursor"),
		},
		{
			name:          "invalid cursor v2",
			cursor:        "2:4::trx1",
			expectedError: fmt.Errorf("invalid cursor"),
		},
		{
			name:          "invalid cursor v1",
			cursor:        "1:4:trx1",
			expectedError: fmt.Errorf("invalid cursor"),
		},
		{
			name:   "empty curso",
			cursor: "",
		},
		//TODO: TEST empty cursor string
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			cursor, err := NewCursorFromString(test.cursor)

			if test.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, test.expectedError, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedCursor, cursor)
			}
		})
	}
}
