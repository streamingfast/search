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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/dfuse-io/derr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
)

func Test_validateQueryFields(t *testing.T) {
	t.Skip("validating is disabled for now because not adapted for ETH, re-enable once settle down")

	tests := []struct {
		in            string
		expectedError error
	}{
		{
			"account:eoscanadacom",
			nil,
		},
		{
			"unknow:eoscanadacom",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'unknow'. Contact our support team for more."),
		},
		{
			"unknow:eoscanadacom second:test",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'second', 'unknow'. Contact our support team for more."),
		},
		{
			"unknow:eoscanadacom account:value second:test",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'second', 'unknow'. Contact our support team for more."),
		},
		{
			"data.from:eoscanadacom data.nested:value account:test",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'data.nested'. Contact our support team for more."),
		},
		{
			"data.from:eoscanadacom data.nested.deep:value account:test",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'data.nested'. Contact our support team for more."),
		},
		{
			"data.from.something:value data.auth.keys.key:value",
			nil,
		},
		{
			"event.field1:value event.field2.nested:value",
			nil,
		},
		{
			"data.from:eoscanadacom data.:value account:test",
			derr.Status(codes.InvalidArgument, "The following fields you are trying to search are not currently indexed: 'data.'. Contact our support team for more."),
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			_, err := NewParsedQuery(pbbstream.Protocol_EOS, test.in)
			if test.expectedError == nil {
				assert.NoError(t, err)
			} else {
				assert.JSONEq(t, toJSONString(t, test.expectedError), toJSONString(t, err))
			}
		})
	}
}

func toJSONString(t *testing.T, v interface{}) string {
	t.Helper()

	out, err := json.Marshal(v)
	require.NoError(t, err)

	return string(out)
}

func fixedTraceID(hexInput string) (out trace.TraceID) {
	rawTraceID, _ := hex.DecodeString(hexInput)
	copy(out[:], rawTraceID)

	return
}
