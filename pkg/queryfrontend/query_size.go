// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

// QuerySizeMiddleware returns a middleware that records the size in bytes of the PromQL 'query'
// parameter and optionally enforces a maximum size.
func QuerySizeMiddleware(maxBytes int, reg prometheus.Registerer) queryrange.Middleware {
	// Histogram of query sizes in bytes.
	// Buckets roughly from 64B up to ~1MB.
	histogram := promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:                           "thanos_query_frontend_query_size_bytes",
		Help:                           "Size in bytes of the PromQL 'query' parameter received by the query frontend.",
		Buckets:                        []float64{64, 128, 256, 512, 1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10, 1024 << 10},
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramMaxBucketNumber: 512,
	})

	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return querySizeMiddleware{
			next:      next,
			maxBytes:  maxBytes,
			histogram: histogram,
		}
	})
}

type querySizeMiddleware struct {
	next      queryrange.Handler
	maxBytes  int
	histogram prometheus.Histogram
}

func (m querySizeMiddleware) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	q := r.GetQuery()
	if q != "" {
		// Record the size in bytes.
		m.histogram.Observe(float64(len(q)))

		// Enforce maximum bytes if configured.
		if m.maxBytes > 0 && len(q) > m.maxBytes {
			// Return a Prometheus-compatible JSON error body.
			bodyStruct := struct {
				Status    string `json:"status"`
				ErrorType string `json:"errorType"`
				Error     string `json:"error"`
			}{
				Status:    "error",
				ErrorType: "bad_data",
				Error:     fmt.Sprintf("query parameter too large: %d bytes > max %d bytes", len(q), m.maxBytes),
			}
			if b, err := json.Marshal(bodyStruct); err == nil {
				return nil, httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
					Code: http.StatusUnprocessableEntity,
					Body: b,
				})
			}
			// Fallback in the unlikely event of JSON marshal failure.
			return nil, httpgrpc.Errorf(http.StatusBadRequest, "query parameter too large: %d bytes > max %d bytes", len(q), m.maxBytes)
		}
	}

	return m.next.Do(ctx, r)
}
