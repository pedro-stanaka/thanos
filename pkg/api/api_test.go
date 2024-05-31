// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/efficientgo/core/testutil"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

func TestRespondSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Respond(w, "test", nil)
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer func() { testutil.Ok(t, resp.Body.Close()) }()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("Return code %d expected in success response but got %d", 200, resp.StatusCode)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/json" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/json", h)
	}

	var res response
	if err = json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("Error unmarshaling JSON body: %s", err)
	}

	exp := &response{
		Status: StatusSuccess,
		Data:   "test",
	}
	if !reflect.DeepEqual(&res, exp) {
		t.Fatalf("Expected response \n%v\n but got \n%v\n", res, exp)
	}
}

func TestRespondError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		RespondError(w, &ApiError{ErrorTimeout, errors.New("message")}, "test")
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer func() { testutil.Ok(t, resp.Body.Close()) }()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if want, have := http.StatusServiceUnavailable, resp.StatusCode; want != have {
		t.Fatalf("Return code %d expected in error response but got %d", want, have)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/json" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/json", h)
	}

	var res response
	if err = json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("Error unmarshaling JSON body: %s", err)
	}

	exp := &response{
		Status:    StatusError,
		Data:      "test",
		ErrorType: ErrorTimeout,
		Error:     "message",
	}
	if !reflect.DeepEqual(&res, exp) {
		t.Fatalf("Expected response \n%v\n but got \n%v\n", res, exp)
	}
}

func TestOptionsMethod(t *testing.T) {
	r := route.New()
	api := &BaseAPI{}
	logMiddleware := logging.NewHTTPServerMiddleware(log.NewNopLogger())
	api.Register(r, &opentracing.NoopTracer{}, log.NewNopLogger(), extpromhttp.NewNopInstrumentationMiddleware(), logMiddleware)

	s := httptest.NewServer(r)
	defer s.Close()

	req, err := http.NewRequest("OPTIONS", s.URL+"/any_path", nil)
	if err != nil {
		t.Fatalf("Error creating OPTIONS request: %s", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Error executing OPTIONS request: %s", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	for h, v := range corsHeaders {
		if resp.Header.Get(h) != v {
			t.Fatalf("Expected %q for header %q, got %q", v, h, resp.Header.Get(h))
		}
	}
}

type fakeQueryData struct {
	ResultType string          `json:"resultType"`
	Result     []promql.Series `json:"result"`
}

// generateSeries generates a series with a given number of samples.
func generateSeries(numSeries, numSamples int) []promql.Series {
	res := make([]promql.Series, 0, numSeries)

	for i := 0; i < numSeries; i++ {
		samples := make([]promql.FPoint, numSamples)
		for j := 0; j < numSamples; j++ {
			samples[j] = promql.FPoint{T: int64(j), F: float64(j * i)}
		}
		res = append(res, promql.Series{
			Metric: labels.Labels{labels.Label{Name: "foo", Value: fmt.Sprintf("bar%d", i)}},
		})
	}

	return res
}

func BenchmarkRespond(b *testing.B) {
	data := fakeQueryData{
		ResultType: "matrix",
	}
	data.Result = generateSeries(30, 5_000)

	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Respond(w, data, nil)
	}
}
