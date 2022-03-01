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

package indexer

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/streamingfast/dgrpc"
	"go.uber.org/zap"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

type healthz struct {
	Ready          bool `json:"ready"`
	HeadBlockDrift int  `json:"head_block_drift_seconds"`
	ShuttingDown   bool `json:"shutting_down"`
}

func (i *Indexer) serveHealthz() {
	// http
	router := mux.NewRouter()
	metricsRouter := router.PathPrefix("/").Subrouter()
	metricsRouter.HandleFunc("/healthz", i.healthzHandler())
	httpServer := &http.Server{Addr: i.httpListenAddr, Handler: router}
	go func() {
		zlog.Info("listening & serving HTTP content", zap.String("http_listen_addr", i.httpListenAddr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zlog.Error("cannot start health check",
				zap.String("http_listen_addr", i.httpListenAddr), zap.Error(err))
		}
	}()

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

func (i *Indexer) healthReport() (out *healthz) {
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

func (i *Indexer) healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := i.healthReport()

		w.Header().Set("Content-Type", "application/json")
		if !h.Ready || h.ShuttingDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(h)
	}
}

func (i *Indexer) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	return &pbhealth.HealthCheckResponse{
		Status: i.healthStatus(),
	}, nil
}

func (i *Indexer) Watch(req *pbhealth.HealthCheckRequest, stream pbhealth.Health_WatchServer) error {
	currentStatus := pbhealth.HealthCheckResponse_SERVICE_UNKNOWN
	waitTime := 0 * time.Second

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(waitTime):
			newStatus := i.healthStatus()
			waitTime = 5 * time.Second

			if newStatus != currentStatus {
				currentStatus = newStatus

				if err := stream.Send(&pbhealth.HealthCheckResponse{Status: currentStatus}); err != nil {
					return err
				}
			}
		}
	}
}

func (i *Indexer) healthStatus() pbhealth.HealthCheckResponse_ServingStatus {
	h := i.healthReport()

	status := pbhealth.HealthCheckResponse_SERVING
	if !h.Ready || h.ShuttingDown {
		status = pbhealth.HealthCheckResponse_NOT_SERVING
	}

	return status
}
