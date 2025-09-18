// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/thanos-io/thanos/internal/cortex/frontend/transport"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

type testHandler struct {
	called bool
}

func (h *testHandler) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	h.called = true
	return queryrange.NewEmptyPrometheusResponse(), nil
}

func TestQuerySizeMiddleware_EnforcesLimit_Instant(t *testing.T) {
	reg := prometheus.NewRegistry()
	mw := QuerySizeMiddleware(10, reg)
	next := &testHandler{}
	h := mw.Wrap(next)

	req := &ThanosQueryInstantRequest{Query: strings.Repeat("a", 20)}
	_, err := h.Do(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for too large query, got nil")
	}
	if next.called {
		t.Fatalf("expected next handler not to be called on enforcement")
	}
}

func TestQuerySizeMiddleware_AllowsUnderLimit_Range(t *testing.T) {
	reg := prometheus.NewRegistry()
	mw := QuerySizeMiddleware(10, reg)
	next := &testHandler{}
	h := mw.Wrap(next)

	req := &ThanosQueryRangeRequest{Query: strings.Repeat("x", 5)}
	_, err := h.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !next.called {
		t.Fatalf("expected next handler to be called")
	}
}

func TestQuerySizeMiddleware_RecordsHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	mw := QuerySizeMiddleware(0, reg)
	next := &testHandler{}
	h := mw.Wrap(next)

	_, _ = h.Do(context.Background(), &ThanosQueryInstantRequest{Query: "abcd"})
	_, _ = h.Do(context.Background(), &ThanosQueryRangeRequest{Query: "abcdefgh"})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}
	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "thanos_query_frontend_query_size_bytes" {
			found = true
			if mf.GetType() != dto.MetricType_HISTOGRAM {
				t.Fatalf("expected histogram type, got %v", mf.GetType())
			}
			if len(mf.GetMetric()) == 0 {
				t.Fatalf("expected at least one histogram metric sample")
			}
			// Check count is >= 2
			if mf.GetMetric()[0].GetHistogram().GetSampleCount() < 2 {
				t.Fatalf("expected histogram count >= 2, got %d", mf.GetMetric()[0].GetHistogram().GetSampleCount())
			}
		}
	}
	if !found {
		t.Fatalf("expected histogram metric thanos_query_frontend_query_size_bytes to be registered")
	}
}

// roundTripperFunc helps stub downstream requests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInstantQuery_Tripperware_EnforcesLimit(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	cfg := Config{}
	cfg.DownstreamURL = "http://downstream"
	cfg.QueryRangeConfig.PartialResponseStrategy = true
	cfg.LabelsConfig.PartialResponseStrategy = true
	cfg.DefaultTimeRange = 0
	cfg.CortexHandlerConfig = &transport.HandlerConfig{MaxBodySize: 1024 * 1024}
	cfg.QueryRangeConfig.MaxQuerySizeBytes = 10

	tripperware, err := NewTripperware(cfg, reg, logger)
	if err != nil {
		t.Fatalf("NewTripperware error: %v", err)
	}

	called := false
	downstream := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})

	rt := tripperware(downstream)

	q := url.Values{}
	q.Set("query", strings.Repeat("q", 50))
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/api/v1/query", RawQuery: q.Encode()},
		Header: http.Header{},
	}

	_, err = rt.RoundTrip(req)
	if err == nil {
		t.Fatalf("expected error for too large query")
	}
	if called {
		t.Fatalf("downstream should not be called when limit is enforced")
	}
}

func TestRangeQuery_Tripperware_EnforcesLimit(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	cfg := Config{}
	cfg.DownstreamURL = "http://downstream"
	cfg.QueryRangeConfig.PartialResponseStrategy = true
	cfg.LabelsConfig.PartialResponseStrategy = true
	cfg.DefaultTimeRange = 0
	cfg.CortexHandlerConfig = &transport.HandlerConfig{MaxBodySize: 1024 * 1024}
	cfg.QueryRangeConfig.MaxQuerySizeBytes = 10

	tripperware, err := NewTripperware(cfg, reg, logger)
	if err != nil {
		t.Fatalf("NewTripperware error: %v", err)
	}

	called := false
	downstream := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})

	rt := tripperware(downstream)

	q := url.Values{}
	q.Set("query", strings.Repeat("q", 50))
	q.Set("start", "0")
	q.Set("end", "1")
	q.Set("step", "1")
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/api/v1/query_range", RawQuery: q.Encode()},
		Header: http.Header{},
	}

	_, err = rt.RoundTrip(req)
	if err == nil {
		t.Fatalf("expected error for too large query")
	}
	if called {
		t.Fatalf("downstream should not be called when limit is enforced")
	}
}
