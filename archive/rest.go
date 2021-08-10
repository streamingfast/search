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
	"fmt"
	"net/http"

	"go.opencensus.io/trace"

	stackdriverPropagation "contrib.go.opencensus.io/exporter/stackdriver/propagation"

	"github.com/streamingfast/derr"
	"github.com/dfuse-io/logging"
	"go.opencensus.io/plugin/ochttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func openCensusMiddleware(next http.Handler) http.Handler {
	return &ochttp.Handler{
		Handler:     next,
		Propagation: &stackdriverPropagation.HTTPFormat{},
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return &logging.Handler{
		Next:        next,
		Propagation: &stackdriverPropagation.HTTPFormat{},
		RootLogger:  zlog,
	}
}

func trackingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zlogger := logging.Logger(r.Context(), zlog)
		zlogger.Debug("handling HTTP request",
			zap.String("method", r.Method),
			zap.Any("host", r.Host),
			zap.Any("url", r.URL),
			zap.Any("headers", r.Header),
		)

		span := trace.FromContext(r.Context())
		if span == nil {
			zlogger.Panic("Trace is not present in request but should have been")
		}

		spanContext := span.SpanContext()
		traceID := spanContext.TraceID.String()

		w.Header().Set("X-Trace-ID", traceID)

		next.ServeHTTP(w, r)
	})
}

func writeJSON(ctx context.Context, w http.ResponseWriter, v interface{}) {
	ctx, span := startSpan(ctx, "write json response", trace.StringAttribute("value_type", fmt.Sprintf("%T", v)))
	defer span.End()

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		level := zapcore.ErrorLevel
		if derr.IsClientSideNetworkError(err) {
			level = zapcore.DebugLevel
		}

		logging.Logger(ctx, zlog).Check(level, "an error occurred while writing response").Write(zap.Error(err))
	}
}

func writeError(ctx context.Context, w http.ResponseWriter, err error) {
	ctx, span := startSpan(ctx, "write error response", trace.StringAttribute("value_type", fmt.Sprintf("%T", err)))
	defer span.End()

	derr.WriteError(ctx, w, "unable to fullfil request", err)
}
