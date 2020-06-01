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

package archive_bigquery

import (
	"context"
	"github.com/dfuse-io/derr"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/search"
)

var LivenessQuery *search.BleveQuery

type healthz struct {
	Ready          bool `json:"ready"`
	HeadBlockDrift int  `json:"head_block_drift_seconds"`
	ShuttingDown   bool `json:"shutting_down"`
}

func (b *ArchiveBigQueryBackend) healthReport() (out *healthz) {
	out = &healthz{
		Ready:        true,
		ShuttingDown: b.shuttingDown.Load(),
	}
	return

}

// GRPC health check endpoint
func (b *ArchiveBigQueryBackend) Check(ctx context.Context, in *pbhealth.HealthCheckRequest) (*pbhealth.HealthCheckResponse, error) {

	h := b.healthReport()
	status := pbhealth.HealthCheckResponse_NOT_SERVING
	if h.Ready && !h.ShuttingDown && !derr.IsShuttingDown() {
		status = pbhealth.HealthCheckResponse_SERVING
	}

	return &pbhealth.HealthCheckResponse{
		Status: status,
	}, nil
}
