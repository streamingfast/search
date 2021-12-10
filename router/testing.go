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

	pb "github.com/streamingfast/pbgo/sf/search/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testBackendClient struct {
	trailer      metadata.MD
	responses    []*pb.SearchMatch
	LastResponse int
	error        error
}

func (c *testBackendClient) StreamMatches(ctx context.Context, in *pb.BackendRequest, opts ...grpc.CallOption) (pb.Backend_StreamMatchesClient, error) {
	return &testBackendStreamer{
		client: c,
	}, nil
}

type testBackendStreamer struct {
	client *testBackendClient
}

func (s *testBackendStreamer) Header() (metadata.MD, error) {
	panic("implement me")
}

func (s *testBackendStreamer) CloseSend() error {
	panic("implement me")
}

func (s *testBackendStreamer) Context() context.Context {
	panic("implement me")
}

func (s *testBackendStreamer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (s *testBackendStreamer) RecvMsg(m interface{}) error {
	panic("implement me")
}

func (s *testBackendStreamer) Trailer() metadata.MD {
	return s.client.trailer
}

func (s *testBackendStreamer) Recv() (*pb.SearchMatch, error) {
	if s.client.LastResponse < len(s.client.responses) {
		match := s.client.responses[s.client.LastResponse]
		s.client.LastResponse++
		return match, nil
	}
	return nil, s.client.error
}

type testPlanner struct {
	plans     []*PeerRange
	planIndex int
	error     error
}

func (p *testPlanner) NextPeer(lowBlockNum uint64, highBlockNum uint64, descending bool, withReversible bool) *PeerRange {
	if p.planIndex == len(p.plans) {
		return nil
	}
	peerRanges := p.plans[p.planIndex]
	p.planIndex += 1
	return peerRanges
}
