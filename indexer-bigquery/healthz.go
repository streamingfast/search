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

package indexer_bigquery

import (
	"context"
	"net"
	"time"

	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/dgrpc"
	"go.uber.org/zap"
)

type healthz struct {
	Ready          bool `json:"ready"`
	HeadBlockDrift int  `json:"head_block_drift_seconds"`
	ShuttingDown   bool `json:"shutting_down"`
}

func (i *IndexerBigQuery) serveHealthz() {
	// gRPC
	gs := dgrpc.NewServer(dgrpc.WithLogger(zlog))
	pbhealth.RegisterHealthServer(gs, i)

	go func() {
		lis, err := net.Listen("tcp", i.grpcListenAddr)
		if err != nil {
			zlog.Error("cannot start health check",
				zap.String("grpc_listen_addr", i.grpcListenAddr), zap.Error(err))
			return
		}

		zlog.Info("listening & serving gRPC content", zap.String("grpc_listen_addr", i.grpcListenAddr))
		if err := gs.Serve(lis); err != nil {
			return
		}
	}()
}

func (i *IndexerBigQuery) healthReport() (out *healthz) {
	//FIXME: implement this
	//headBlockTime := b.pool.GetLiveIndexHeadBlockTime()
	headBlockTime := time.Now()

	out = &healthz{
		Ready:          i.isReady(),
		HeadBlockDrift: int(time.Since(headBlockTime) / time.Second),
		ShuttingDown:   i.shuttingDown.Load(),
	}

	if out.HeadBlockDrift < 0 {
		zlog.Error("healthz - live head block_time is in the future?!", zap.Time("now", time.Now()), zap.Time("head_block_time", headBlockTime), zap.Int("drift_secs", out.HeadBlockDrift))
	}
	return

}

func (i *IndexerBigQuery) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {

	h := i.healthReport()

	status := pbhealth.HealthCheckResponse_SERVING
	if !h.Ready || h.ShuttingDown {
		status = pbhealth.HealthCheckResponse_NOT_SERVING
	}

	return &pbhealth.HealthCheckResponse{
		Status: status,
	}, nil
}
