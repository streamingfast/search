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
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/streamingfast/derr"
	pb "github.com/streamingfast/pbgo/dfuse/search/v1"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func Test_run(t *testing.T) {
	tests := []struct {
		name                  string
		ctx                   context.Context
		request               *pb.BackendRequest
		client                *testBackendClient
		expectedError         error
		expectedLastBlockRead int64
	}{
		{
			name: "finds and matches and correctly",
			ctx:  context.Background(),
			request: &pb.BackendRequest{
				LowBlockNum:  5,
				HighBlockNum: 12,
			},
			client: &testBackendClient{
				trailer: metadata.Pairs(
					"range-completed", "true",
					"last-block-read", "11",
				),
				responses: []*pb.SearchMatch{
					{TrxIdPrefix: "a", BlockNum: 10, Index: 0},
					{TrxIdPrefix: "b", BlockNum: 10, Index: 1},
					{TrxIdPrefix: "c", BlockNum: 11, Index: 2},
				},
				error: io.EOF,
			},
			expectedLastBlockRead: 11,
		},
		{
			name: "validates backend bounds correctly",
			ctx:  context.Background(),
			request: &pb.BackendRequest{
				LowBlockNum:  5,
				HighBlockNum: 12,
			},
			client: &testBackendClient{
				trailer: metadata.Pairs(
					"range-completed", "true",
					"last-block-read", "11",
				),
				responses: []*pb.SearchMatch{
					{TrxIdPrefix: "a", BlockNum: 4, Index: 0},
				},
				error: io.EOF,
			},
			expectedError: fmt.Errorf("received search match at block num 4 from backend client outside of backedn query range [5,12]"),
		},
		{
			name: "validates backend bounds correctly",
			ctx:  context.Background(),
			request: &pb.BackendRequest{
				LowBlockNum:  5,
				HighBlockNum: 12,
			},
			client: &testBackendClient{
				trailer: metadata.Pairs(
					"range-completed", "true",
					"last-block-read", "11",
				),
				responses: []*pb.SearchMatch{
					{TrxIdPrefix: "a", BlockNum: 7, Index: 0},
					{TrxIdPrefix: "b", BlockNum: 15, Index: 0},
				},
				error: io.EOF,
			},
			expectedError: fmt.Errorf("received search match at block num 15 from backend client outside of backedn query range [5,12]"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backendQuery := newBackendQuery(test.client, test.request)
			err := backendQuery.run(test.ctx, zap.NewNop(), testSenderFilter(test.client))
			if test.expectedError != nil {
				assert.Equal(t, err, test.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, backendQuery.LastBlockRead, test.expectedLastBlockRead)
			}
		})
	}
}

func Test_ContextCanc(t *testing.T) {
	ctx := context.Background()
	backendClient := &testBackendClient{
		trailer: metadata.Pairs(
			"range-completed", "true",
			"last-block-read", "11",
		),
		responses: []*pb.SearchMatch{
			{TrxIdPrefix: "a", BlockNum: 10, Index: 0},
			{TrxIdPrefix: "b", BlockNum: 10, Index: 1},
			{TrxIdPrefix: "c", BlockNum: 11, Index: 2},
			{TrxIdPrefix: "d", BlockNum: 11, Index: 2},
		},
		error: io.EOF,
	}
	backendRequest := &pb.BackendRequest{
		LowBlockNum:  5,
		HighBlockNum: 12,
	}

	backendQuery := newBackendQuery(backendClient, backendRequest)
	err := backendQuery.run(ctx, zap.NewNop(), testSenderFilterWithCancelledContext(backendClient))
	assert.Equal(t, err, derr.Status(codes.Canceled, "context canceled"))
}

func testSenderFilter(client *testBackendClient) func(*pb.SearchMatch) error {
	return func(*pb.SearchMatch) error {
		return nil
	}
}

func testSenderFilterWithCancelledContext(client *testBackendClient) func(*pb.SearchMatch) error {
	return func(*pb.SearchMatch) error {
		return context.Canceled
	}
}
