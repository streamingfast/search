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

package archive

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/streamingfast/derr"
	"github.com/streamingfast/search"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

var LivenessQuery *search.BleveQuery

type healthz struct {
	Ready          bool `json:"ready"`
	HeadBlockDrift int  `json:"head_block_drift_seconds"`
	ShuttingDown   bool `json:"shutting_down"`
}

func (b *ArchiveBackend) healthReport() (out *healthz) {
	out = &healthz{
		Ready:        b.Pool.IsReady(),
		ShuttingDown: b.shuttingDown.Load(),
	}
	return

}

// HTTP health check endpoint
func (b *ArchiveBackend) healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := b.healthReport()
		w.Header().Set("Content-Type", "application/json")
		if !h.Ready || h.ShuttingDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(h)
	}
}

func (b *ArchiveBackend) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {
	return &pbhealth.HealthCheckResponse{
		Status: b.healthStatus(),
	}, nil
}

func (b *ArchiveBackend) Watch(req *pbhealth.HealthCheckRequest, stream pbhealth.Health_WatchServer) error {
	currentStatus := pbhealth.HealthCheckResponse_SERVICE_UNKNOWN
	waitTime := 0 * time.Second

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(waitTime):
			newStatus := b.healthStatus()
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

func (b *ArchiveBackend) healthStatus() pbhealth.HealthCheckResponse_ServingStatus {
	h := b.healthReport()

	status := pbhealth.HealthCheckResponse_NOT_SERVING
	if h.Ready && !h.ShuttingDown && !derr.IsShuttingDown() {
		status = pbhealth.HealthCheckResponse_SERVING
	}

	return status
}
