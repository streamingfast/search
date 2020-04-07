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

	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
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
	status := pbhealth.HealthCheckResponse_SERVING
	return &pbhealth.HealthCheckResponse{
		Status: status,
	}, nil
}
