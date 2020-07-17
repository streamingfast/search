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

package live

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/search"
	"go.uber.org/zap"
)

func (b *LiveBackend) SetupSubscriptionHub(
	startBlock bstream.BlockRef,
	blockFilter func(blk *bstream.Block) error,
	blockMapper search.BlockMapper,
	blocksStore dstore.Store,
	blockstreamAddr string,
	liveIndexesPath string,
	realtimeTolerance time.Duration,
	truncationThreshold int,
	preProcConcurrentThreads int,
) error {
	zlog.Info("setting up subscription hub")

	if truncationThreshold < 1 {
		return fmt.Errorf("invalid truncation thershold %d, value must be greater then 0", truncationThreshold)
	}

	p := search.NewPreIndexer(blockMapper, liveIndexesPath)
	// this indexes the block directly from the live source (relayer) and the file source (100-blocks)... it happens before the
	// realtime tolerance... ouch
	preprocessor := bstream.PreprocessFunc(p.Preprocess)

	liveSourceFactory := bstream.SourceFromNumFactory(func(_ uint64, h bstream.Handler) bstream.Source {
		src := blockstream.NewSource(context.Background(), blockstreamAddr, 300, bstream.NewPreprocessor(preprocessor, h), blockstream.WithRequester("search-live"))
		src.SetParallelPreproc(preprocessor, preProcConcurrentThreads)
		return src
	})

	filePreprocessor := bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
		if blockFilter != nil {
			err := blockFilter(blk)
			if err != nil {
				return nil, fmt.Errorf("block filter: %w", err)
			}
		}

		return preprocessor(blk)
	})

	fileSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		return bstream.NewFileSource(blocksStore, startBlockNum, 1, filePreprocessor, h)
	})

	logger := zlog.Named("hub")
	buffer := bstream.NewBuffer("archive-hub", logger)
	tailManager := NewTailManager(b.dmeshClient.Peers, b.dmeshClient, b.searchPeer, buffer, 300, truncationThreshold, startBlock)
	subscriptionHub, err := hub.NewSubscriptionHub(
		startBlock.Num(), // condition it needs to be 2 or greater
		buffer,
		tailManager.TailLock,
		fileSourceFactory,
		liveSourceFactory,
		hub.Withlogger(logger),
		hub.WithRealtimeTolerance(realtimeTolerance),
		hub.WithSourceChannelSize(1000), // FIXME: we should not need this, but when the live kicks in, we receive too many blocks at once on the progressPeerPublishing...
		// maybe an option on the hub to "skip blocks if the channel is full" should apply, but that would be only on that specific subscription
	)
	if err != nil {
		return err
	}

	go tailManager.Launch()

	go b.launchBlockProgressPeerPublishing(subscriptionHub)

	b.tailManager = tailManager
	b.hub = subscriptionHub
	zlog.Info("setting realtime tolerance on subscriptionHub", zap.Duration("realtime_tolerance", realtimeTolerance))

	go subscriptionHub.Launch()

	return nil
}

func (b *LiveBackend) launchBlockProgressPeerPublishing(hub *hub.SubscriptionHub) {
	dmeshHandler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		fObj := obj.(*forkable.ForkableObject)
		switch fObj.Step {
		case forkable.StepNew:
			zlog.Debug("step new in launchBlockProgressPeerPublishing", zap.Uint64("block_num", blk.Num()))
			headBlockTimeDrift.SetBlockTime(blk.Time())

			b.searchPeer.Locked(func() {
				b.searchPeer.HeadBlock = blk.Number
				b.searchPeer.HeadBlockID = blk.Id
			})
			b.dmeshClient.PublishWithin(b.searchPeer, 1*time.Second)
			headBlockNumber.SetUint64(blk.Num())

		case forkable.StepIrreversible:
			zlog.Debug("step irreversible in launchBlockProgressPeerPublishing", zap.Uint64("block_num", blk.Num()))

			b.searchPeer.Locked(func() {
				b.searchPeer.IrrBlock = blk.Number
				b.searchPeer.IrrBlockID = blk.Id
			})
			b.dmeshClient.PublishWithin(b.searchPeer, 1*time.Second)

		default:
			panic("should hit other Step types here")
		}

		return nil
	})

	fk := forkable.New(dmeshHandler, forkable.WithLogger(zlog), forkable.WithFilters(forkable.StepNew|forkable.StepIrreversible))
	src := hub.NewSource(fk, 0)

	b.OnTerminating(src.Shutdown)
	// TODO: wrap in an Eternal source?
	src.Run()
	if err := src.Err(); err != nil {
		b.Shutdown(fmt.Errorf("block progress thread failed, would not have moved anymore. Implement an eternal handler?: %w", err))
	}
}

func (b *LiveBackend) WaitHubReady() {
	zlog.Info("waiting for subscription hub to be ready")

	b.hub.WaitUntilRealTime()

	zlog.Info("subscription hub is real-time")

	for b.hub.HeadBlock() == nil || !b.tailManager.Ready() {
		time.Sleep(time.Second)
	}

	zlog.Info("subscription hub and buffer ready")
}
