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

	"github.com/dfuse-io/derr"
	pbsearch "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func Test_newQueryRange(t *testing.T) {

	tests := []struct {
		name                  string
		request               *pbsearch.RouterRequest
		cursor                *cursor
		head                  uint64
		irr                   uint64
		headDelayTolerance    uint64
		libDelayTolerance     uint64
		expectedHigh          uint64
		expectedLow           uint64
		truncationLowBlockNum int64
		expectedMode          pbsearch.RouterRequest_Mode
		expectedError         error
	}{
		//------------------------
		//Without a cursor
		//------------------------
		//Legacy tests
		{
			name: "legacy, forward 1",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          200,
				Descending:          false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  100,
			expectedHigh: 300,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, forward kept over lib ",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          5000,
				Descending:          false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  100,
			expectedHigh: 5100,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, forward kept over head ",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          5000,
				Descending:          false,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  100,
			expectedHigh: 5100,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			//TODO: expectedHigh is a bit off due ot MaxInt cs MaxUint.
			name: "legacy, forward with huge block count is still OK",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          uint64(UnboundedInt),
				Descending:          false,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  100,
			expectedHigh: uint64(UnboundedInt),
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward 1",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          300,
				BlockCount:          200,
				Descending:          true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  100,
			expectedHigh: 300,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward illegal over lib",
			head: 2000,
			irr:  1000,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          1500,
				BlockCount:          600,
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond LIB, use `0` to follow LIB (requested: 1500, lib: 1000)"),
		},
		{
			name: "legacy, backward illegal over head",
			head: 2000,
			irr:  1000,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          2500,
				BlockCount:          600,
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond LIB, use `0` to follow LIB (requested: 2500, lib: 1000)"),
		},
		{
			name: "legacy, forward illegal over lib",
			head: 2000,
			irr:  1000,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          1500,
				BlockCount:          10,
				Descending:          false,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond LIB, use `0` to follow LIB (requested: 1500, lib: 1000)"),
		},
		{
			name: "legacy, forward illegal over head",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          2500,
				BlockCount:          10,
				Descending:          false,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond HEAD block, use `0` to follow the HEAD (requested: 2500, head: 2000)"),
		},
		{
			name: "legacy, forward illegal under truncationLow",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          900,
				BlockCount:          10,
				Descending:          false,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 1000,
			expectedError:         derr.Status(codes.InvalidArgument, "invalid start block: 900 is lower than the lowest block served by this endpoint [1000]"),
		},
		{
			name: "legacy, backward illegal under truncationLow",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          900,
				BlockCount:          10,
				Descending:          true,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 1000,
			expectedError:         derr.Status(codes.InvalidArgument, "invalid start block: 900 is lower than the lowest block served by this endpoint [1000]"),
		},

		{
			name: "legacy, backward with huge block count is still OK",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          uint64(UnboundedInt), //default and max legacy value
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  1,
			expectedHigh: 100,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward with huge block count is still OK, truncated silently",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          100,
				BlockCount:          uint64(UnboundedInt), //default and max legacy value
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 10,
			expectedLow:           10,
			expectedHigh:          100,
			expectedMode:          pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward from 0 is head",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          0,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  1900,
			expectedHigh: 2000,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward from 0 is head, truncated silently",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          0,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      true,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 1925,
			expectedLow:           1925,
			expectedHigh:          2000,
			expectedMode:          pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward from 0 is lib",
			head: 2000,
			irr:  1000,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          0,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			expectedLow:  900,
			expectedHigh: 1000,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name: "legacy, backward from 0 is lib, truncated silently",
			head: 2000,
			irr:  1000,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          0,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      false,
				Mode:                pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 950,
			expectedLow:           950,
			expectedHigh:          1000,
			expectedMode:          pbsearch.RouterRequest_PAGINATED,
		},

		// Ascending tests
		// [L,H]
		{
			name: "asc, low block and high block explicitly set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 101,
			expectedLow:  20,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block explicitly set and equal",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   47,
				LowBlockNum:    47,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 47,
			expectedLow:  47,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block explicitly set and invalid",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   20,
				LowBlockNum:    101,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond low block, value was 20"),
		},
		{
			name: "asc, low block and high block explicitly set, high blocks exceeds head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   5000,
				LowBlockNum:    101,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 5000,
			expectedLow:  101,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block 0 and truncation set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    0,
				WithReversible: true,
				Descending:     false,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: -1000,
			expectedError:         derr.Status(codes.InvalidArgument, "invalid low block num on ascending request: 0 is lower than the lowest block served by this endpoint [1000]"),
		},
		{
			name: "asc, low block unbounded and truncation set starts on truncation",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -100,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        false,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: -1000,
			expectedHigh:          1901,
			expectedLow:           1000,
			expectedMode:          pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block explicitly set, irreversible only, and high block exceeds IRR",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				HighBlockNum: 1900,
				LowBlockNum:  20,
				Mode:         pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1900,
			expectedLow:  20,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		//[-L,-H]
		{
			name: "asc, low block and high block relatively set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -1733,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 501,
			expectedLow:  268,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block relatively set and equal ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1622,
				LowBlockNum:    -1622,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 379,
			expectedLow:  379,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block explicitly set and invalid",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1733,
				LowBlockNum:    -1500,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond low block, value was -1733"),
		},
		{
			name: "asc, low block and high block relatively set where low block resolves to less then 0",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -3000,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid low block num: goes beyond first block, value was -3000"),
		},
		{
			name: "asc, low block and high block relatively set where high block resolves to less then 0",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -3000,
				LowBlockNum:    -2000,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond first block, value was -3000"),
		},
		{
			name: "asc, low block and high block relatively set, irreversible only, valid range",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				HighBlockNum: -1500,
				LowBlockNum:  -1733,
				Mode:         pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 301,
			expectedLow:  68,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "asc, low block and high block relatively set, irreversible only, and low block resolves below 0",
			head: 2000,
			irr:  1800,
			request: &pbsearch.RouterRequest{
				HighBlockNum: -1500,
				LowBlockNum:  -1823,
				Mode:         pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid low block num: goes beyond first block, value was -1823"),
		},
		// [-L,-1]
		{
			name: "asc, low block relatively set and high block set at head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1,
				LowBlockNum:    -1500,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  501,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [-1,-1]
		{
			name: "asc, low block and high block set at head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1,
				LowBlockNum:    -1,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  2000,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,None]
		{
			name: "asc, low block and high block are unbounded ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockUnbounded:  true,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None, H]
		{
			name: "asc, low block  is unbounded and high block is set explicitly ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      1980,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1980,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [L,None]
		{
			name: "asc, low block is  explicitly set and high block is unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockNum:        382,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  382,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,-H]
		{
			name: "asc, low block is unbounded and high block is relatively set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -239,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1762,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,-1]
		{
			name: "asc, low block is unbounded and high block is relatively set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -1,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [-L,None]
		{
			name: "asc, low block is relatively set and and high block is  unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				LowBlockNum:        -239,
				HighBlockUnbounded: true,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  1762,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [-1,None]
		{
			name: "asc, low block is relatively set to head and and high block is  unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				LowBlockNum:        -1,
				HighBlockUnbounded: true,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  2000,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// Descending
		// [L,H]
		{
			name: "desc, low block and high block explicitly set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 101,
			expectedLow:  20,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block and high block explicitly set and equal",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   47,
				LowBlockNum:    47,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 47,
			expectedLow:  47,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block and high block explicitly set and invalid",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   20,
				LowBlockNum:    101,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond low block, value was 20"),
		},
		{
			name: "desc, low block and high block explicitly set, head block exceeds head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   5000,
				LowBlockNum:    101,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: it goes beyond the current head block, use `-1` for to follow the HEAD (requested: 5000, head: 2000)"),
		},
		// [-L,-H]
		{
			name: "desc, low block and high block relatively set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -1733,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 501,
			expectedLow:  268,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block and high block relatively set and equal ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1622,
				LowBlockNum:    -1622,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 379,
			expectedLow:  379,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block and high block explicitly set and invalid",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1733,
				LowBlockNum:    -1500,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond low block, value was -1733"),
		},
		{
			name: "desc, low block and high block relatively set where low block resolves to less then 0",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -3000,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid low block num: goes beyond first block, value was -3000"),
		},
		{
			name: "desc, low block and high block relatively set where high block resolves to less then 0",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -3000,
				LowBlockNum:    -2000,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: goes beyond first block, value was -3000"),
		},
		// [-L,-1]
		{
			name: "desc, low block relatively set and high block set at head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1,
				LowBlockNum:    -1500,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  501,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [-1,-1]
		{
			name: "desc, low block and high block set at head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1,
				LowBlockNum:    -1,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  2000,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,None]
		{
			name: "desc, low block and high block are unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockUnbounded:  true,
				WithReversible:     true,
				Descending:         true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block and high block are unbounded, irreversible",
			head: 2000,
			irr:  1900,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockUnbounded:  true,
				Descending:         true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1900,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None, H]
		{
			name: "desc, low block is unbounded and high block is set explicitly ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      1980,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1980,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "desc, low block below truncated",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   1980,
				LowBlockNum:    1,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 1000,
			expectedError:         derr.Status(codes.InvalidArgument, "invalid low block num on descending request: 1 is lower than the lowest block served by this endpoint [1000]"),
		},
		{
			name: "desc, low unbounded becomes truncated",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      1980,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: 1000,
			expectedHigh:          1980,
			expectedLow:           1000,
			expectedMode:          pbsearch.RouterRequest_STREAMING,
		},

		// [L,None]
		{
			name: "desc, low block is explicitly set and high block is unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockNum:        382,
				WithReversible:     true,
				Descending:         true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  382,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		//[None,-H]
		{
			name: "desc, low block is unbounded and high block is relatively set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -239,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 1762,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		//[-L,None]
		{
			name: "desc, low block is relatively set and and high block is  unbounded, error cannot have descending unbounded high",
			head: 2000,
			request: &pbsearch.RouterRequest{
				LowBlockNum:        -239,
				HighBlockUnbounded: true,
				WithReversible:     true,
				Descending:         true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			expectedHigh: 2000,
			expectedLow:  1762,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		//------------------------
		// With a cursor
		//------------------------
		// Ascending
		{
			name: "with cursor, asc, low block and high block explicitly set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 43},
			expectedHigh: 101,
			expectedLow:  43,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "with cursor, asc, low block and high block explicitly set and valid and cursor is below low block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 12},
			expectedHigh: 101,
			expectedLow:  12,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "with cursor, asc, low block and high block explicitly set and valid and cursor is above height block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:        &cursor{blockNum: 203},
			expectedError: derr.Status(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (203) is out of requested block range [20-101]."),
		},
		// [-L,-H]
		{
			name: "with cursor, asc, low block and high block relatively set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -1733,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 312},
			expectedHigh: 501,
			expectedLow:  312,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [-L,-1]
		{
			name: "with cursor, asc, low block relatively set and high block set at head",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1,
				LowBlockNum:    -1500,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 829},
			expectedHigh: 2000,
			expectedLow:  829,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,None]
		{
			name: "with cursor, asc, low block and high block are  unbounded ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockUnbounded:  true,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 829},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  829,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None, H]
		{
			name: "with cursor, asc, low block  is unbounded and high block is set explicitly ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      1980,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 829},
			expectedHigh: 1980,
			expectedLow:  829,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [L,None]
		{
			name: "with cursor, asc, low block is explicitly set and high block is unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				LowBlockNum:        382,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 829},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  829,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,-H]
		{
			name: "with cursor, asc, low block is unbounded and high block is relatively set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -239,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 829},
			expectedHigh: 1762,
			expectedLow:  829,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		//[-L,None]
		{
			name: "with cursor, desc, low block is relatively set and and high block is unbounded",
			head: 2000,
			request: &pbsearch.RouterRequest{
				LowBlockNum:        -239,
				HighBlockUnbounded: true,
				WithReversible:     true,
				Descending:         true,
				Mode:               pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 1923},
			expectedHigh: 1923,
			expectedLow:  1762,
		},
		// Descending
		// [L,H]
		{
			name: "with cursor, desc, low block and high block explicitly set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 82},
			expectedHigh: 82,
			expectedLow:  20,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		{
			name: "with cursor, desc, low block and high block explicitly set and valid and cursor is below low block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:        &cursor{blockNum: 12},
			expectedError: derr.Status(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (12) is out of requested block range [20-101]."),
		},
		{
			name: "with cursor, desc, low block and high block explicitly set, but low block below truncation low block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    2,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: -1950,
			cursor:                &cursor{blockNum: 12},
			expectedError:         derr.Status(codes.InvalidArgument, "invalid low block num on descending request: 2 is lower than the lowest block served by this endpoint [50]"),
		},
		{
			name: "with cursor, desc, low block and high block explicitly set and valid but cursor is below truncation low block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      101,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			truncationLowBlockNum: -1950,
			cursor:                &cursor{blockNum: 12},
			expectedError:         derr.Status(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (12) is out of requested block range [50-101]."),
		},
		{
			name: "with cursor, desc, low block and high block explicitly set and valid and cursor is above height block",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   101,
				LowBlockNum:    20,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:        &cursor{blockNum: 203},
			expectedError: derr.Status(codes.InvalidArgument, "the query you are trying to perform is not valid, the cursor block num (203) is out of requested block range [20-101]."),
		},
		// [-L,-H]
		{
			name: "with cursor, desc, low block and high block relatively set and valid ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   -1500,
				LowBlockNum:    -1733,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 453},
			expectedHigh: 453,
			expectedLow:  268,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None, H]
		{
			name: "with cursor, desc, low block  is unbounded and high block is set explicitly ",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      1980,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 837},
			expectedHigh: 837,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// [None,-H]
		{
			name: "with cursor, desc, low block is unbounded and high block is relatively set",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockNum:      -239,
				LowBlockUnbounded: true,
				WithReversible:    true,
				Descending:        true,
				Mode:              pbsearch.RouterRequest_STREAMING,
			},
			cursor:       &cursor{blockNum: 837},
			expectedHigh: 837,
			expectedLow:  0,
			expectedMode: pbsearch.RouterRequest_STREAMING,
		},
		// prod error 2019
		{
			name: "low bound relative, high unbounded, paginated with a limit",
			head: 2000,
			request: &pbsearch.RouterRequest{
				HighBlockUnbounded: true,
				Limit:              100,
				LowBlockNum:        -100,
				WithReversible:     true,
				Mode:               pbsearch.RouterRequest_PAGINATED,
			},
			expectedHigh: uint64(UnboundedInt),
			expectedLow:  1901,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		//testing tolerance
		{
			name:               "descending request from the future with reversible within toleration",
			head:               2000,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   2002,
				Limit:          100,
				LowBlockNum:    1500,
				WithReversible: true,
				Mode:           pbsearch.RouterRequest_PAGINATED,
			},
			expectedHigh: 2002,
			expectedLow:  1500,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name:               "descending request from the future with reversible above toleration",
			head:               2000,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   2005,
				Limit:          100,
				LowBlockNum:    1500,
				WithReversible: true,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_PAGINATED,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: it goes beyond the current head block, use `-1` for to follow the HEAD (requested: 2005, head: 2000)"),
		},
		{
			name:               "descending request from the future without reversible within toleration",
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   1806,
				Limit:          100,
				LowBlockNum:    1500,
				WithReversible: false,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_PAGINATED,
			},
			expectedHigh: 1806,
			expectedLow:  1500,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name:               "descending request from the future without reversible above toleration",
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				HighBlockNum:   1818,
				Limit:          100,
				LowBlockNum:    1500,
				WithReversible: false,
				Descending:     true,
				Mode:           pbsearch.RouterRequest_PAGINATED,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid high block num: it goes beyond the current last irreversible block, use `-1` to follow the LIB (requested: 1818, lib: 1800)"),
		},
		// legacy testing tolerance
		{
			name:               "legacy descending request from the future with reversible within toleration",
			head:               2000,
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          2002,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      true,
			},
			expectedHigh: 2002,
			expectedLow:  1902,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name:               "legacy descending request from the future with reversible above toleration",
			head:               2000,
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          2004,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      true,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond HEAD block, use `0` to follow the HEAD (requested: 2004, head: 2000)"),
		},
		{
			name:               "legacy descending request from the future without reversible within toleration",
			head:               2000,
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          1806,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      false,
			},
			expectedHigh: 1806,
			expectedLow:  1706,
			expectedMode: pbsearch.RouterRequest_PAGINATED,
		},
		{
			name:               "legacy descending request from the future without reversible above toleration",
			head:               2000,
			irr:                1800,
			headDelayTolerance: 3,
			libDelayTolerance:  12,
			request: &pbsearch.RouterRequest{
				UseLegacyBoundaries: true,
				StartBlock:          1832,
				BlockCount:          100,
				Descending:          true,
				WithReversible:      false,
			},
			expectedError: derr.Status(codes.InvalidArgument, "invalid start block num: goes beyond LIB, use `0` to follow LIB (requested: 1832, lib: 1800)"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queryRange, err := newQueryRange(test.request, test.cursor, test.head, test.irr, test.headDelayTolerance, test.libDelayTolerance, test.truncationLowBlockNum)

			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedHigh, queryRange.highBlockNum)
				assert.Equal(t, test.expectedLow, queryRange.lowBlockNum)
				assert.Equal(t, test.expectedMode, queryRange.mode)
			}
		})
	}
}
