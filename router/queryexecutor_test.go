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

	pb "github.com/streamingfast/pbgo/sf/search/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// Hoes does the stream end when there is
type testInboundStream struct {
	matches []*pb.SearchMatch
}

type testQuerySharder struct {
	name                  string
	planner               *testPlanner
	backendClientsWrapper map[string]*testBackendClient
	request               *pb.RouterRequest
	queryRange            *QueryRange
	cursor                *cursor
	expectedPlanLoops     int
	expectedMatches       []*pb.SearchMatch
	expectedLimitedReach  bool
	expectedTrxCount      int64
}

func Test_QueryRangeIncomplete(t *testing.T) {
	tests := []testQuerySharder{
		{
			name: "simple ascending query with 1 backend range",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedPlanLoops:    1,
			expectedTrxCount:     3,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple ascending query with 2 backend ranges",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "230"),
				},
			},
			expectedPlanLoops:    2,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
			},
		},
		{
			name: "simple ascending  query with 2 backend ranges, where one ends earlier then expected",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-0-1a", LowBlockNum: 93, HighBlockNum: 100},
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
				},
				error: nil,
			},

			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "93"),
				},
				"archive-tier-0-1a": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "c", BlockNum: 95, Index: 34},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "230"),
				},
			},
			expectedPlanLoops:    3,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 95, Index: 34},
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
			},
		},
		{
			name: "simple descending query with 1 backend range",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedPlanLoops:    1,
			expectedTrxCount:     3,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple descending query with 2 backend ranges",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "101"),
				},
			},
			expectedPlanLoops:    2,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple descending query with multi backend ranges, where one ends earlier then expected",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
					{Addr: "archive-tier-1-1a", LowBlockNum: 101, HighBlockNum: 116},
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-0-1a", LowBlockNum: 20, HighBlockNum: 92},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "117"),
				},
				"archive-tier-1-1a": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "f", BlockNum: 106, Index: 7},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "101"),
				},
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "93"),
				},
				"archive-tier-0-1a": {
					responses: []*pb.SearchMatch{},
					error:     io.EOF,
					trailer:   metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedPlanLoops:    4,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
				{TrxIdPrefix: "f", BlockNum: 106, Index: 7},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			inboundStream := &testInboundStream{}
			sharder := newQueryExecutor(ctx, test.request, test.planner, test.cursor, test.queryRange, zlog, newTestBackendClient(&test), newBackendQuery, newTestStreamSend(inboundStream))
			sharder.Query()
			assert.Equal(t, test.expectedMatches, inboundStream.matches)
			assert.Equal(t, test.expectedPlanLoops, test.planner.planIndex)
			assert.Equal(t, test.expectedTrxCount, sharder.trxCount)
			assert.Equal(t, test.expectedLimitedReach, sharder.limitReached)
		})
	}
}

func Test_QueryBasicFlow(t *testing.T) {
	tests := []testQuerySharder{
		{
			name: "simple ascending query with 1 backend range",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedPlanLoops:    1,
			expectedTrxCount:     3,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple ascending query with 2 backend ranges",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "230"),
				},
			},
			expectedPlanLoops:    2,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
			},
		},
		{
			name: "simple ascending query with 2 backend ranges, where one ends earlier then expected",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-0-1a", LowBlockNum: 93, HighBlockNum: 100},
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
				},
				error: nil,
			},

			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "93"),
				},
				"archive-tier-0-1a": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "c", BlockNum: 95, Index: 34},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "230"),
				},
			},
			expectedPlanLoops:    3,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 95, Index: 34},
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
			},
		},
		{
			name: "simple descending query with 1 backend range",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedPlanLoops:    1,
			expectedTrxCount:     3,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple descending query with 2 backend ranges",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
						{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "101"),
				},
			},
			expectedPlanLoops:    2,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
				{TrxIdPrefix: "c", BlockNum: 100, Index: 4},
			},
		},
		{
			name: "simple descending query with multi backend ranges, where one ends earlier then expected",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 230,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 230},
					{Addr: "archive-tier-1-1a", LowBlockNum: 101, HighBlockNum: 116},
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-0-1a", LowBlockNum: 20, HighBlockNum: 92},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
						{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "117"),
				},
				"archive-tier-1-1a": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "f", BlockNum: 106, Index: 7},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "101"),
				},
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "93"),
				},
				"archive-tier-0-1a": {
					responses: []*pb.SearchMatch{},
					error:     io.EOF,
					trailer:   metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedPlanLoops:    4,
			expectedTrxCount:     5,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 132, Index: 32},
				{TrxIdPrefix: "e", BlockNum: 212, Index: 78},
				{TrxIdPrefix: "f", BlockNum: 106, Index: 7},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 123},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 763},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			inboundStream := &testInboundStream{}
			sharder := newQueryExecutor(ctx, test.request, test.planner, test.cursor, test.queryRange, zlog, newTestBackendClient(&test), newBackendQuery, newTestStreamSend(inboundStream))
			require.NoError(t, sharder.Query())
			assert.Equal(t, test.expectedMatches, inboundStream.matches)
			assert.Equal(t, test.expectedPlanLoops, test.planner.planIndex)
			assert.Equal(t, test.expectedTrxCount, sharder.trxCount)
			assert.Equal(t, test.expectedLimitedReach, sharder.limitReached)
		})
	}
}

func Test_Cursor(t *testing.T) {
	tests := []testQuerySharder{
		{
			name: "sharder should send matches once the cursor block is passed on ascending query",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			cursor: &cursor{
				blockNum: 34,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
						{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
						{TrxIdPrefix: "d", BlockNum: 62, Index: 39},
						{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 62, Index: 39},
				{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
			},
		},
		{
			name: "sharder should send matches once the cursor block & trx prefix is passed on ascending query",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			cursor: &cursor{
				blockNum:  34,
				trxPrefix: "c",
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
						{TrxIdPrefix: "b", BlockNum: 34, Index: 16},
						{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
						{TrxIdPrefix: "d", BlockNum: 34, Index: 39},
						{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "d", BlockNum: 34, Index: 39},
				{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
			},
		},
		{
			name: "sharder should send matches once the cursor block is passed on descending query",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			cursor: &cursor{
				blockNum: 34,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
						{TrxIdPrefix: "d", BlockNum: 62, Index: 39},
						{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
			},
		},
		{
			name: "sharder should send matches once the cursor block & trx prefix is passed on ascending query",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      0,
			},
			cursor: &cursor{
				blockNum:  34,
				trxPrefix: "c",
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
						{TrxIdPrefix: "d", BlockNum: 34, Index: 39},
						{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
						{TrxIdPrefix: "b", BlockNum: 34, Index: 16},
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "20"),
				},
			},
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "b", BlockNum: 34, Index: 16},
				{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			inboundStream := &testInboundStream{}
			sharder := newQueryExecutor(ctx, test.request, test.planner, test.cursor, test.queryRange, zlog, newTestBackendClient(&test), newBackendQuery, newTestStreamSend(inboundStream))
			sharder.Query()
			assert.Equal(t, test.expectedMatches, inboundStream.matches)
		})
	}
}

func Test_Limit(t *testing.T) {
	tests := []testQuerySharder{
		{
			name: "sharder limit the matches based on request limit",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      4,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedTrxCount:     2,
			expectedLimitedReach: false,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
			},
		},
		{
			name: "sharder limit the matches based on request limit",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      4,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 100,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
						{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
						{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
						{TrxIdPrefix: "d", BlockNum: 62, Index: 39},
						{TrxIdPrefix: "e", BlockNum: 87, Index: 83},
						{TrxIdPrefix: "f", BlockNum: 88, Index: 85},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
			},
			expectedTrxCount:     4,
			expectedLimitedReach: true,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
				{TrxIdPrefix: "b", BlockNum: 24, Index: 16},
				{TrxIdPrefix: "c", BlockNum: 34, Index: 18},
				{TrxIdPrefix: "d", BlockNum: 62, Index: 39},
			},
		},
		{
			name: "sharder limit the matches based on request limit over multiple backends",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
				Limit:      4,
			},
			queryRange: &QueryRange{
				lowBlockNum:  20,
				highBlockNum: 140,
				mode:         pb.RouterRequest_STREAMING,
			},
			planner: &testPlanner{
				plans: []*PeerRange{
					{Addr: "archive-tier-0-1", LowBlockNum: 20, HighBlockNum: 100},
					{Addr: "archive-tier-1-1", LowBlockNum: 101, HighBlockNum: 140},
				},
				error: nil,
			},
			backendClientsWrapper: map[string]*testBackendClient{
				"archive-tier-0-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
						{TrxIdPrefix: "b", BlockNum: 34, Index: 16},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "100"),
				},
				"archive-tier-1-1": {
					responses: []*pb.SearchMatch{
						{TrxIdPrefix: "c", BlockNum: 110, Index: 18},
						{TrxIdPrefix: "d", BlockNum: 113, Index: 39},
						{TrxIdPrefix: "e", BlockNum: 123, Index: 83},
					},
					error:   io.EOF,
					trailer: metadata.Pairs("last-block-read", "140"),
				},
			},
			expectedTrxCount:     4,
			expectedLimitedReach: true,
			expectedMatches: []*pb.SearchMatch{
				{TrxIdPrefix: "a", BlockNum: 20, Index: 13},
				{TrxIdPrefix: "b", BlockNum: 34, Index: 16},
				{TrxIdPrefix: "c", BlockNum: 110, Index: 18},
				{TrxIdPrefix: "d", BlockNum: 113, Index: 39},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			inboundStream := &testInboundStream{}
			sharder := newQueryExecutor(ctx, test.request, test.planner, test.cursor, test.queryRange, zlog, newTestBackendClient(&test), newBackendQuery, newTestStreamSend(inboundStream))
			sharder.Query()
			assert.Equal(t, test.expectedMatches, inboundStream.matches)
			assert.Equal(t, test.expectedLimitedReach, sharder.limitReached)
			assert.Equal(t, test.expectedTrxCount, sharder.trxCount)
		})
	}
}

func Test_createBackendQuery(t *testing.T) {
	tests := []struct {
		name                 string
		peer                 *PeerRange
		request              *pb.RouterRequest
		cursor               *cursor
		queryRange           *QueryRange
		expectBackendRequest *pb.BackendRequest
	}{
		{
			name: "bounded, ascending streaming request, with live backend (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: false,
				Mode:       pb.RouterRequest_STREAMING,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_STREAMING,
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      200,
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        false,
				StopAtVirtualHead: false,
			},
		},
		{
			name: "bounded, ascending streaming request, with live backend (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:              "action:onblock",
				Descending:         false,
				Mode:               pb.RouterRequest_STREAMING,
				WithReversible:     true,
				LiveMarkerInterval: 20,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_STREAMING,
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:       200,
				LowBlockNum:        20,
				Query:              "action:onblock",
				Descending:         false,
				StopAtVirtualHead:  false,
				WithReversible:     true,
				LiveMarkerInterval: 20,
			},
		},
		{
			name: "bounded, ascending paginated request, with live backend (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     false,
				Mode:           pb.RouterRequest_PAGINATED,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      200,
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        false,
				StopAtVirtualHead: true,
				WithReversible:    true,
			},
		},
		{
			name: "unbounded, ascending streaming request, with live backend (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     false,
				Mode:           pb.RouterRequest_STREAMING,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_STREAMING,
			},
			peer: &PeerRange{
				HighBlockNum:     uint64(UnboundedInt),
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      uint64(UnboundedInt),
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        false,
				StopAtVirtualHead: false,
				WithReversible:    true,
			},
		},
		{
			name: "unbounded, ascending paginated request, with live backend (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     false,
				Mode:           pb.RouterRequest_PAGINATED,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     uint64(UnboundedInt),
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      uint64(UnboundedInt),
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        false,
				StopAtVirtualHead: true,
				WithReversible:    true,
			},
		},
		{
			name: "bounded, ascending paginated request, with live backend & cursor (serving reversible blk)",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     false,
				Mode:           pb.RouterRequest_PAGINATED,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			cursor: &cursor{
				blockNum:    20,
				headBlockID: "abc123",
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:         200,
				LowBlockNum:          20,
				Query:                "action:onblock",
				Descending:           false,
				StopAtVirtualHead:    true,
				WithReversible:       true,
				NavigateFromBlockID:  "abc123",
				NavigateFromBlockNum: 20,
			},
		},
		{
			name: "bounded, descending paginated request, with archive backend",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_PAGINATED,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: false,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      200,
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        true,
				StopAtVirtualHead: false,
			},
		},
		{
			name: "bounded, descending paginated request, with archive backend with cursor",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_PAGINATED,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: false,
			},
			cursor: &cursor{
				blockNum:    20,
				headBlockID: "abc123",
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:         200,
				LowBlockNum:          20,
				Query:                "action:onblock",
				Descending:           true,
				StopAtVirtualHead:    false,
				NavigateFromBlockID:  "",
				NavigateFromBlockNum: 0,
			},
		},
		{
			name: "bounded, descending paginated request, with live backend",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     true,
				Mode:           pb.RouterRequest_PAGINATED,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      200,
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        true,
				StopAtVirtualHead: false,
				WithReversible:    true,
			},
		},
		{
			name: "unbounded, descending paginated request, with live backend",
			request: &pb.RouterRequest{
				Query:          "action:onblock",
				Descending:     true,
				Mode:           pb.RouterRequest_PAGINATED,
				WithReversible: true,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     uint64(UnboundedInt),
				LowBlockNum:      20,
				ServesReversible: true,
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:      uint64(UnboundedInt),
				LowBlockNum:       20,
				Query:             "action:onblock",
				Descending:        true,
				StopAtVirtualHead: false,
				WithReversible:    true,
			},
		},
		{
			name: "bounded, descending paginated request, with live backend & cursor",
			request: &pb.RouterRequest{
				Query:      "action:onblock",
				Descending: true,
				Mode:       pb.RouterRequest_PAGINATED,
			},
			queryRange: &QueryRange{
				mode: pb.RouterRequest_PAGINATED, // this is set equal to routerRequest.Mode
			},
			peer: &PeerRange{
				HighBlockNum:     200,
				LowBlockNum:      20,
				ServesReversible: true,
			},
			cursor: &cursor{
				blockNum:    20,
				headBlockID: "abc123",
			},
			expectBackendRequest: &pb.BackendRequest{
				HighBlockNum:         200,
				LowBlockNum:          20,
				Query:                "action:onblock",
				Descending:           true,
				StopAtVirtualHead:    false,
				NavigateFromBlockID:  "abc123",
				NavigateFromBlockNum: 20,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			sharder := queryExecutor{
				ctx:        ctx,
				request:    test.request,
				cur:        test.cursor,
				queryRange: test.queryRange,
			}

			backendQuery := sharder.createBackendQuery(test.peer)
			require.Equal(t, test.expectBackendRequest, backendQuery)
		})
	}
}

func newTestStreamSend(t *testInboundStream) func(*pb.SearchMatch) error {
	return func(match *pb.SearchMatch) error {
		t.matches = append(t.matches, match)
		return nil
	}
}

func newTestBackendClient(t *testQuerySharder) func(peerRange *PeerRange) pb.BackendClient {
	return func(peerRange *PeerRange) pb.BackendClient {
		if v, found := t.backendClientsWrapper[peerRange.Addr]; found {
			return v
		} else {
			panic(fmt.Sprintf("test should always have a backendClient associated to each peer range [%s]", peerRange.Addr))
		}
		return nil
	}
}
