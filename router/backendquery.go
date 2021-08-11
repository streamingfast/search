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
	"strconv"

	"github.com/streamingfast/derr"
	pb "github.com/streamingfast/pbgo/dfuse/search/v1"
	"github.com/streamingfast/search/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

///
/// Archive query
///

type backendClientFactory func(peerRange *PeerRange) pb.BackendClient
type backendQueryFactory func(client pb.BackendClient, request *pb.BackendRequest) *BackendQuery

func newBackendClient(peerRange *PeerRange) pb.BackendClient {
	return pb.NewBackendClient(peerRange.Conn)
}

type BackendQuery struct {
	client  pb.BackendClient   // change to LiveClient eventually
	request *pb.BackendRequest // include the `sortDesc`, necesssary here

	LastBlockRead int64
}

func newBackendQuery(client pb.BackendClient, request *pb.BackendRequest) *BackendQuery {
	return &BackendQuery{
		client:  client,
		request: request,
	}
}

func (q *BackendQuery) run(ctx context.Context, zlogger *zap.Logger, streamSend func(*pb.SearchMatch) error) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := q.client.StreamMatches(ctx, q.request)
	if err != nil {
		if isGrpcCancellationError(err) {
			return derr.Status(codes.Canceled, "context canceled")
		}
		return err
	}

	for {
		msg, err := resp.Recv()
		if err == io.EOF {
			trailer := resp.Trailer()
			if x := trailer.Get("last-block-read"); len(x) > 0 {
				if x[0] == "-1" {
					return fmt.Errorf("backend last-block-read is -1, backend should returned an error")
				}

				if y, err := strconv.ParseInt(x[0], 10, 64); err == nil {
					q.LastBlockRead = y
					return nil
				}
			}
			// Every search backend is expected to return a "last-block-read" trailer
			zlogger.Warn("missing last-block-read trailer from search backend", zap.Reflect("backend", q.client))
			return fmt.Errorf("missing last-block-read trailer from search backend")
		}

		if isGrpcCancellationError(err) {
			return derr.Status(codes.Canceled, "context canceled")
		}

		if err != nil {
			metrics.ErrorBackendCount.Inc()
			zlogger.Warn("error receiving message from backend stream client", zap.Error(err))
			return err
		}

		if !within(msg.BlockNum, q.request.LowBlockNum, q.request.HighBlockNum) {
			zlogger.Error("received search match block num from backend client outside of backend query range",
				zap.Reflect("search_match", msg),
				zap.Uint64("low_block_num", q.request.LowBlockNum),
				zap.Uint64("high_block_num", q.request.HighBlockNum))
			return fmt.Errorf("received search match at block num %d from backend client outside of backedn query range [%d,%d]", msg.BlockNum, q.request.LowBlockNum, q.request.HighBlockNum)
		}

		err = streamSend(msg)

		if isGrpcCancellationError(err) {
			return derr.Status(codes.Canceled, "context canceled")
		}

		if err != nil {
			return err
		}
	}
}
