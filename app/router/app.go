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
	"time"

	"github.com/streamingfast/search/metrics"

	"github.com/streamingfast/dgrpc"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	dmeshClient "github.com/streamingfast/dmesh/client"
	"github.com/streamingfast/search/router"
	"go.uber.org/zap"
)

type Config struct {
	ServiceVersion        string // dmesh service version (v1)
	BlockmetaAddr         string // Blockmeta endpoint is queried to validate cursors that are passed LIB and forked out
	GRPCListenAddr        string // Address to listen for incoming gRPC requests
	HeadDelayTolerance    uint64 // Number of blocks above a backend's head we allow a request query to be served (Live & Router)
	LibDelayTolerance     uint64 // Number of blocks above a backend's lib we allow a request query to be served (Live & Router)
	EnableRetry           bool   // Enable the router's attempt to retry a backend search if there is an error. This could have adverse consequences when search through the live
	TruncationLowBlockNum int64  // Lowest block num expected to be served, can be relative to head
}

type Modules struct {
	Dmesh dmeshClient.SearchClient
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
	zlog.Info("running router app ", zap.Reflect("config", a.config))

	metrics.Register(metrics.RouterMetricSet)

	// TODO: the router does not need the information in registry
	//if err := search.ValidateRegistry(); err != nil {
	//	return err
	//}

	zlog.Info("starting dmesh")
	err := a.modules.Dmesh.Start(context.Background(), []string{
		"/" + a.config.ServiceVersion + "/search",
	})
	if err != nil {
		return fmt.Errorf("unable to start dmesh client: %w", err)
	}

	conn, err := dgrpc.NewInternalClient(a.config.BlockmetaAddr)
	if err != nil {
		return fmt.Errorf("getting blockmeta client: %w", err)
	}

	blockmetaCli := pbblockmeta.NewBlockIDClient(conn)
	forksCli := pbblockmeta.NewForksClient(conn)

	router := router.New(a.modules.Dmesh, a.config.HeadDelayTolerance, a.config.LibDelayTolerance, blockmetaCli, forksCli, a.config.EnableRetry, a.config.TruncationLowBlockNum)

	a.OnTerminating(router.Shutdown)
	router.OnTerminated(a.Shutdown)

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	zlog.Info("launching router")
	go router.Launch(a.config.GRPCListenAddr)

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
