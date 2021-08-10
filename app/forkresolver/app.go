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

package forkresolver

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dgrpc"
	"github.com/streamingfast/dstore"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/dmesh"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search"
	"github.com/streamingfast/search/forkresolver"
	"github.com/streamingfast/search/metrics"
	"go.uber.org/zap"
)

type Config struct {
	ServiceVersion  string        // dmesh service version (v1)
	GRPCListenAddr  string        // Address to listen for incoming gRPC requests
	HttpListenAddr  string        // Address to listen for incoming http requests
	PublishInterval time.Duration // longest duration a dmesh peer will not publish
	IndicesPath     string        // Location for inflight indices
	BlocksStoreURL  string        // Path to read blocks archives
}

type Modules struct {
	BlockFilter func(blk *bstream.Block) error
	BlockMapper search.BlockMapper
	Dmesh       dmeshClient.SearchClient
}

type App struct {
	*shutter.Shutter
	config         *Config
	modules        *Modules
	readinessProbe pbhealth.HealthClient
}

func New(config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
	}
}

func (a *App) Run() error {
	zlog.Info("running forkresolver app ", zap.Reflect("config", a.config))

	metrics.Register(metrics.ForkResolverMetricSet)

	if err := search.ValidateRegistry(); err != nil {
		return err
	}

	zlog.Info("starting dmesh")
	err := a.modules.Dmesh.Start(context.Background(), []string{
		"/" + a.config.ServiceVersion + "/search",
	})
	if err != nil {
		return fmt.Errorf("unable to start dmesh client: %w", err)
	}

	blocksStore, err := dstore.NewDBinStore(a.config.BlocksStoreURL)
	if err != nil {
		return fmt.Errorf("failed setting up blocks store: %w", err)
	}

	zlog.Info("creating search peer")
	searchPeer := dmesh.NewSearchForkResolverPeer(a.config.ServiceVersion, a.config.GRPCListenAddr, a.config.PublishInterval)

	zlog.Info("publishing search archive peer", zap.String("peer_host", searchPeer.GenericPeer.Host))
	err = a.modules.Dmesh.PublishNow(searchPeer)
	if err != nil {
		return fmt.Errorf("publishing peer to dmesh: %w", err)
	}

	fr := forkresolver.NewForkResolver(
		blocksStore,
		a.modules.Dmesh,
		searchPeer,
		a.config.GRPCListenAddr,
		a.config.HttpListenAddr,
		a.modules.BlockFilter,
		a.modules.BlockMapper,
		a.config.IndicesPath)

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	a.OnTerminating(fr.Shutdown)
	fr.OnTerminated(a.Shutdown)

	zlog.Info("launching forkresolver search")
	go fr.Launch()

	return nil
}

func (a *App) IsReady() bool {
	if a.readinessProbe == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := a.readinessProbe.Check(ctx, &pbhealth.HealthCheckRequest{})
	if err != nil {
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}
