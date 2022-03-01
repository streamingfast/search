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
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

func (f *ForkResolver) serveHealthz() {

	// http
	router := mux.NewRouter()
	metricsRouter := router.PathPrefix("/").Subrouter()
	metricsRouter.HandleFunc("/healthz", f.healthzHandler())
	httpServer := &http.Server{Addr: f.httpListenAddr, Handler: router}
	go func() {
		zlog.Info("listening & serving HTTP content", zap.String("http_listen_addr", f.httpListenAddr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zlog.Error("cannot start health check",
				zap.String("http_listen_addr", f.httpListenAddr), zap.Error(err))
		}
	}()
}

type healthz struct {
	Ready        bool `json:"ready"`
	ShuttingDown bool `json:"shutting_down"`
}

func (f *ForkResolver) healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := &healthz{
			Ready: true,
		}

		w.Header().Set("Content-Type", "application/json")
		if !h.Ready || h.ShuttingDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(h)
	}
}

func (f *ForkResolver) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	return &pbhealth.HealthCheckResponse{
		Status: f.healthStatus(),
	}, nil
}

func (f *ForkResolver) Watch(req *pbhealth.HealthCheckRequest, stream pbhealth.Health_WatchServer) error {
	currentStatus := pbhealth.HealthCheckResponse_SERVICE_UNKNOWN
	waitTime := 0 * time.Second

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(waitTime):
			newStatus := f.healthStatus()
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

func (f *ForkResolver) healthStatus() pbhealth.HealthCheckResponse_ServingStatus {
	return pbhealth.HealthCheckResponse_SERVING
}
