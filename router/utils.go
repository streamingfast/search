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
	"errors"

	"github.com/dfuse-io/dmesh"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var LimitReached = errors.New("limit reached")
var ContextCanceled = errors.New("context canceled")
var InvalidArgument = errors.New("invalid argument")

func within(block, lowBlockNum, highBlockNum uint64) bool {
	return (lowBlockNum <= block) && (block <= highBlockNum)
}

func getReadyPeers(peers []*dmesh.SearchPeer) (out []*dmesh.SearchPeer) {
	for _, peer := range peers {
		if peer.Ready {
			if peer.VirtualHead() > 0 {
				zlog.Debug("available peer", zap.Reflect("search_peer", peer))
				out = append(out, peer)
			} else {
				zlog.Debug("skipping ready peer with virtualhead == 0", zap.Reflect("search_peer", peer))
			}
		}
	}
	return
}

func isGrpcCancellationError(err error) bool {
	if err == context.Canceled {
		return true
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Canceled {
			return true
		}
	}
	return false
}
