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

	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	pbsearch "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/dmesh"
	"github.com/dfuse-io/search/metrics"
	"go.uber.org/zap"
)

type forkExecutor struct {
	ctx          context.Context
	zlogger      *zap.Logger
	peerFetcher  func() []*dmesh.SearchPeer
	forkClient   pbblockmeta.ForksClient
	forkedCursor *cursor
	reqQuery     string
	streamSend   func(*pbsearch.SearchMatch) error
}

func newForkExecutor(ctx context.Context, forkClient pbblockmeta.ForksClient, peerFetcher func() []*dmesh.SearchPeer, logger *zap.Logger, cur *cursor, query string, streamSend func(*pbsearch.SearchMatch) error) *forkExecutor {
	return &forkExecutor{
		ctx:          ctx,
		zlogger:      logger,
		peerFetcher:  peerFetcher,
		forkClient:   forkClient,
		forkedCursor: cur,
		reqQuery:     query,
		streamSend:   streamSend,
	}
}

func (f *forkExecutor) query() (lowBlockNum int64, trxcount int64, err error) {

	resolvers := f.getResolvers()
	if len(resolvers) == 0 {
		f.zlogger.Warn("no fork resolver found")
		return 0, 0, InvalidArgument
	}

	forkResolutionPath, err := f.getForkedCursorResolution()
	if err != nil {
		f.zlogger.Warn("cannot get resolution from forks client", zap.Error(err))
		return 0, 0, InvalidArgument
	}

	if len(forkResolutionPath) == 0 {
		f.zlogger.Warn("fork resolution path is empty, probably because the cursor is not forked")
		return 0, 0, InvalidArgument
	}

	var req *pbsearch.ForkResolveRequest
	req, lowBlockNum = f.createForkResolverRequest(forkResolutionPath)

	//for _, resolver := range resolvers { // TODO implement multi resolvers
	resolver := resolvers[0]
	trxcount, err = f.runResolver(req, resolver)
	if err != nil {
		if err == ContextCanceled {
			return
		}
		err = fmt.Errorf("error running fork resolver: %w", err)
		return
	}
	//}
	return
}

func (f *forkExecutor) getResolvers() (out []*dmesh.SearchPeer) {
	readyPeers := getReadyPeers(f.peerFetcher())
	for _, peer := range readyPeers {
		if peer.ServesResolveForks {
			out = append(out, peer)
		}
	}
	return
}

func (f *forkExecutor) getForkedCursorResolution() ([]*pbblockmeta.BlockRef, error) {
	req := &pbblockmeta.ForkResolveRequest{
		Block: &pbblockmeta.BlockRef{
			BlockNum: f.forkedCursor.blockNum,
			BlockID:  f.forkedCursor.headBlockID,
		},
	}
	resp, err := f.forkClient.Resolve(f.ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ForkedBlockRefs, nil
}

func (f *forkExecutor) createForkResolverRequest(blockRefs []*pbblockmeta.BlockRef) (req *pbsearch.ForkResolveRequest, lowestBlockNum int64) {
	if len(blockRefs) == 0 {
		return
	}
	req = &pbsearch.ForkResolveRequest{
		Query:           f.reqQuery,
		ForkedBlockRefs: []*pbsearch.BlockRef{},
	}
	lowestBlockNum = int64(blockRefs[0].BlockNum)
	for _, blkRef := range blockRefs {
		if int64(blkRef.BlockNum) < lowestBlockNum {
			lowestBlockNum = int64(blkRef.BlockNum)
		}
		req.ForkedBlockRefs = append(req.ForkedBlockRefs, &pbsearch.BlockRef{
			BlockNum: blkRef.BlockNum,
			BlockID:  blkRef.BlockID,
		})
	}
	return
}

func (f *forkExecutor) runResolver(req *pbsearch.ForkResolveRequest, resolver *dmesh.SearchPeer) (trxcount int64, err error) {
	resolverCli := pbsearch.NewForkResolverClient(resolver.Conn())
	resp, err := resolverCli.StreamUndoMatches(f.ctx, req)
	if err != nil {
		if isGrpcCancellationError(err) {
			return 0, ContextCanceled
		}
		return 0, err
	}

	for {
		msg, err := resp.Recv()
		if err == io.EOF {
			break
		}

		if isGrpcCancellationError(err) {
			return trxcount, ContextCanceled
		}

		if err != nil {
			metrics.ErrorBackendCount.Inc()
			f.zlogger.Info("error receiving message from backend stream client", zap.Error(err))
			return trxcount, err
		}
		trxcount++
		if err := f.streamSend(msg); err != nil {
			if isGrpcCancellationError(err) {
				return trxcount, ContextCanceled
			}
			return trxcount, err
		}
	}
	return
}
