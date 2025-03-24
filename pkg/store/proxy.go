// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type ctxKey int

// UninitializedTSDBTime is the TSDB start time of an uninitialized TSDB instance.
const UninitializedTSDBTime = math.MaxInt64

// StoreMatcherKey is the context key for the store's allow list.
const StoreMatcherKey = ctxKey(0)

// ErrorNoStoresMatched is returned if the query does not match any data.
// This can happen with Query servers trees and external labels.
var ErrorNoStoresMatched = errors.New("No StoreAPIs matched for this query")

// Client holds meta information about a store.
type Client interface {
	// StoreClient to access the store.
	storepb.StoreClient

	// LabelSets that each apply to some data exposed by the backing store.
	LabelSets() []labels.Labels

	// TimeRange returns minimum and maximum time range of data in the store.
	TimeRange() (mint int64, maxt int64)

	// GuaranteedMinTime returns the minimum time that a store always guarantees to have.
	GuaranteedMinTime() int64

	// TSDBInfos returns metadata about each TSDB backed by the client.
	TSDBInfos() []infopb.TSDBInfo

	// SupportsSharding returns true if sharding is supported by the underlying store.
	SupportsSharding() bool

	// SupportsWithoutReplicaLabels returns true if trimming replica labels
	// and sorted response is supported by the underlying store.
	SupportsWithoutReplicaLabels() bool

	// String returns the string representation of the store client.
	String() string

	// Addr returns address of the store client. If second parameter is true, the client
	// represents a local client (server-as-client) and has no remote address.
	Addr() (addr string, isLocalClient bool)
}

// ProxyStore implements the store API that proxies request to all given underlying stores.
type ProxyStore struct {
	logger         log.Logger
	stores         func() []Client
	component      component.StoreAPI
	selectorLabels labels.Labels
	buffers        sync.Pool

	responseTimeout           time.Duration
	metrics                   *proxyStoreMetrics
	retrievalStrategy         RetrievalStrategy
	debugLogging              bool
	propagateSelectorMatchers bool
	storeSelector             *storeSelector
	enableDedup               bool
}

type proxyStoreMetrics struct {
	emptyStreamResponses prometheus.Counter
}

func newProxyStoreMetrics(reg prometheus.Registerer) *proxyStoreMetrics {
	var m proxyStoreMetrics

	m.emptyStreamResponses = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_proxy_store_empty_stream_responses_total",
		Help: "Total number of empty responses received.",
	})

	return &m
}

func RegisterStoreServer(storeSrv storepb.StoreServer, logger log.Logger) func(*grpc.Server) {
	return func(s *grpc.Server) {
		storepb.RegisterStoreServer(s, NewRecoverableStoreServer(logger, storeSrv))
	}
}

// BucketStoreOption are functions that configure BucketStore.
type ProxyStoreOption func(s *ProxyStore)

// WithProxyStoreDebugLogging enables debug logging.
func WithProxyStoreDebugLogging() ProxyStoreOption {
	return func(s *ProxyStore) {
		s.debugLogging = true
	}
}

// WithProxyStoreRelabelConfig configures a store relabel config.
func WithProxyStoreRelabelConfig(relabelConfig []*relabel.Config) ProxyStoreOption {
	return func(s *ProxyStore) {
		s.storeSelector = &storeSelector{relabelConfig: relabelConfig}
	}
}

// PropagateStoreSelectorMatchers configures .
func PropagateStoreSelectorMatchers(value bool) ProxyStoreOption {
	return func(s *ProxyStore) {
		s.propagateSelectorMatchers = value
	}
}

// WithoutDedup disabled chunk deduplication when streaming series.
func WithoutDedup() ProxyStoreOption {
	return func(s *ProxyStore) {
		s.enableDedup = false
	}
}

// NewProxyStore returns a new ProxyStore that uses the given clients that implements storeAPI to fan-in all series to the client.
// Note that there is no deduplication support. Deduplication should be done on the highest level (just before PromQL).
func NewProxyStore(
	logger log.Logger,
	reg prometheus.Registerer,
	stores func() []Client,
	component component.StoreAPI,
	selectorLabels labels.Labels,
	responseTimeout time.Duration,
	retrievalStrategy RetrievalStrategy,
	options ...ProxyStoreOption,
) *ProxyStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	metrics := newProxyStoreMetrics(reg)
	s := &ProxyStore{
		logger:         logger,
		stores:         stores,
		component:      component,
		selectorLabels: selectorLabels,
		buffers: sync.Pool{New: func() interface{} {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},

		responseTimeout:           responseTimeout,
		metrics:                   metrics,
		retrievalStrategy:         retrievalStrategy,
		propagateSelectorMatchers: true,
		storeSelector:             newStoreSelector(nil),
		enableDedup:               true,
	}

	for _, option := range options {
		option(s)
	}

	return s
}

func (s *ProxyStore) LabelSet() []labelpb.ZLabelSet {
	stores := s.stores()
	if len(stores) == 0 {
		return []labelpb.ZLabelSet{}
	}

	mergedLabelSets := make(map[uint64]labelpb.ZLabelSet, len(stores))
	for _, st := range stores {
		for _, lset := range st.LabelSets() {
			mergedLabelSet := labelpb.ExtendSortedLabels(lset, s.selectorLabels)
			mergedLabelSets[mergedLabelSet.Hash()] = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(mergedLabelSet)}
		}
	}

	labelSets := make([]labelpb.ZLabelSet, 0, len(mergedLabelSets))
	for _, v := range mergedLabelSets {
		labelSets = append(labelSets, v)
	}

	// We always want to enforce announcing the subset of data that
	// selector-labels represents. If no label-sets are announced by the
	// store-proxy's discovered stores, then we still want to enforce
	// announcing this subset by announcing the selector as the label-set.
	selectorLabels := labelpb.ZLabelsFromPromLabels(s.selectorLabels)
	if len(labelSets) == 0 && len(selectorLabels) > 0 {
		labelSets = append(labelSets, labelpb.ZLabelSet{Labels: selectorLabels})
	}

	return labelSets
}

func (s *ProxyStore) TimeRange() (int64, int64) {
	stores := s.stores()
	if len(stores) == 0 {
		return math.MinInt64, math.MaxInt64
	}

	var minTime, maxTime int64 = math.MaxInt64, math.MinInt64
	for _, s := range stores {
		storeMinTime, storeMaxTime := s.TimeRange()
		if storeMinTime < minTime {
			minTime = storeMinTime
		}
		if storeMaxTime > maxTime {
			maxTime = storeMaxTime
		}
	}

	return minTime, maxTime
}

func (s *ProxyStore) GuaranteedMinTime() int64 {
	stores := s.stores()
	if len(stores) == 0 {
		return UninitializedTSDBTime
	}

	var mint int64 = math.MinInt64
	for _, s := range stores {
		storeMint := s.GuaranteedMinTime()
		if storeMint == UninitializedTSDBTime {
			continue
		}
		if storeMint > mint {
			mint = storeMint
		}
	}

	return mint
}

func (s *ProxyStore) TSDBInfos() []infopb.TSDBInfo {
	infos := make([]infopb.TSDBInfo, 0)
	for _, store := range s.stores() {
		infos = append(infos, store.TSDBInfos()...)
	}
	return infos
}

func (s *ProxyStore) Series(originalRequest *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	// TODO(bwplotka): This should be part of request logger, otherwise it does not make much sense. Also, could be
	// tiggered by tracing span to reduce cognitive load.
	reqLogger := log.With(s.logger, "component", "proxy")
	if s.debugLogging {
		reqLogger = log.With(reqLogger, "request", originalRequest.String())
	}

	match, matchers, err := matchesExternalLabels(originalRequest.Matchers, s.selectorLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding selector labels)").Error())
	}
	storeMatchers, _ := storepb.PromMatchersToMatchers(matchers...) // Error would be returned by matchesExternalLabels, so skip check.

	storeDebugMsgs := []string{}
	r := &storepb.SeriesRequest{
		MinTime:                 originalRequest.MinTime,
		MaxTime:                 originalRequest.MaxTime,
		Matchers:                storeMatchers,
		Aggregates:              originalRequest.Aggregates,
		MaxResolutionWindow:     originalRequest.MaxResolutionWindow,
		SkipChunks:              originalRequest.SkipChunks,
		QueryHints:              originalRequest.QueryHints,
		PartialResponseDisabled: originalRequest.PartialResponseDisabled,
		PartialResponseStrategy: originalRequest.PartialResponseStrategy,
		ShardInfo:               originalRequest.ShardInfo,
		WithoutReplicaLabels:    originalRequest.WithoutReplicaLabels,
	}

	var stores []Client
	for _, st := range s.stores() {
		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(srv.Context(), st, s.debugLogging, originalRequest.MinTime, originalRequest.MaxTime, matchers...); !ok {
			if s.debugLogging {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s filtered out: %v", st, reason))
			}
			continue
		}

		stores = append(stores, st)
	}

	stores, selectorMatchers := s.storeSelector.matchStores(stores, r.WithoutReplicaLabels)
	if s.propagateSelectorMatchers {
		r.Matchers = append(r.Matchers, selectorMatchers...)
	} else {
		r.Matchers = removeExternalLabelMatchers(stores, r.Matchers)
	}

	if len(stores) == 0 {
		level.Debug(reqLogger).Log("err", ErrorNoStoresMatched, "stores", strings.Join(storeDebugMsgs, ";"))
		return nil
	}

	storeResponses := make([]respSet, 0, len(stores))
	for _, st := range stores {
		st := st
		if s.debugLogging {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s queried", st))
		}

		respSet, err := newAsyncRespSet(srv.Context(), st, r, s.responseTimeout, s.retrievalStrategy, &s.buffers, r.ShardInfo, reqLogger, s.metrics.emptyStreamResponses)
		if err != nil {
			level.Error(reqLogger).Log("err", err)

			if !r.PartialResponseDisabled || r.PartialResponseStrategy == storepb.PartialResponseStrategy_WARN {
				if err := srv.Send(storepb.NewWarnSeriesResponse(err)); err != nil {
					return err
				}
				continue
			} else {
				return err
			}
		}

		storeResponses = append(storeResponses, respSet)
		defer respSet.Close()
	}

	_ = level.Debug(reqLogger).Log("msg", "Series: started fanout streams", "status", strings.Join(storeDebugMsgs, ";"))

	var respHeap seriesStream = NewProxyResponseHeap(storeResponses...)
	if s.enableDedup {
		respHeap = NewDedupResponseHeap(respHeap)
	}
	for respHeap.Next() {
		resp := respHeap.At()

		if resp.GetWarning() != "" && (r.PartialResponseDisabled || r.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT) {
			return status.Error(codes.Aborted, resp.GetWarning())
		}

		if err := srv.Send(resp); err != nil {
			_ = level.Error(reqLogger).Log("msg", "failed to send series", "err", err)
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	return nil
}

func removeExternalLabelMatchers(stores []Client, matchers []storepb.LabelMatcher) []storepb.LabelMatcher {
	var (
		newMatchers []storepb.LabelMatcher
		extLset     = make(map[string]struct{})
	)
	for _, store := range stores {
		for _, labelSets := range store.LabelSets() {
			labelSets.Range(func(lbl labels.Label) {
				extLset[lbl.Name] = struct{}{}

			})
		}
	}
	for _, m := range matchers {
		if _, ok := extLset[m.Name]; !ok {
			newMatchers = append(newMatchers, m)
		}
	}
	return newMatchers
}

// storeMatches returns boolean if the given store may hold data for the given label matchers, time ranges and debug store matches gathered from context.
func storeMatches(ctx context.Context, s Client, debugLogging bool, mint, maxt int64, matchers ...*labels.Matcher) (ok bool, reason string) {
	var storeDebugMatcher [][]*labels.Matcher
	if ctxVal := ctx.Value(StoreMatcherKey); ctxVal != nil {
		if value, ok := ctxVal.([][]*labels.Matcher); ok {
			storeDebugMatcher = value
		}
	}

	storeMinTime, storeMaxTime := s.TimeRange()
	if mint > storeMaxTime || maxt < storeMinTime {
		if debugLogging {
			reason = fmt.Sprintf("does not have data within this time period: [%v,%v]. Store time ranges: [%v,%v]", mint, maxt, storeMinTime, storeMaxTime)
		}
		return false, reason
	}

	if ok, reason := storeMatchDebugMetadata(s, storeDebugMatcher); !ok {
		return false, reason
	}

	extLset := s.LabelSets()
	if !labelSetsMatch(matchers, extLset...) {
		if debugLogging {
			reason = fmt.Sprintf("external labels %v does not match request label matchers: %v", extLset, matchers)
		}
		return false, reason
	}
	return true, ""
}

// storeMatchDebugMetadata return true if the store's address match the storeDebugMatchers.
func storeMatchDebugMetadata(s Client, storeDebugMatchers [][]*labels.Matcher) (ok bool, reason string) {
	if len(storeDebugMatchers) == 0 {
		return true, ""
	}

	addr, isLocal := s.Addr()
	if isLocal {
		return false, "the store is not remote, cannot match __address__"
	}

	match := false
	for _, sm := range storeDebugMatchers {
		match = match || labelSetsMatch(sm, labels.FromStrings("__address__", addr))
	}
	if !match {
		return false, fmt.Sprintf("__address__ %v does not match debug store metadata matchers: %v", addr, storeDebugMatchers)
	}
	return true, ""
}

// labelSetsMatch returns false if all label-set do not match the matchers (aka: OR is between all label-sets).
func labelSetsMatch(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		notMatched := false
		for _, m := range matchers {
			if lv := ls.Get(m.Name); ls.Has(m.Name) && !m.Matches(lv) {
				notMatched = true
				break
			}
		}
		if !notMatched {
			return true
		}
	}
	return false
}

// LabelNames returns all known label names.
func (s *ProxyStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	var (
		warnings       []string
		names          [][]string
		mtx            sync.Mutex
		g, gctx        = errgroup.WithContext(ctx)
		storeDebugMsgs []string
	)

	for _, st := range s.stores() {
		st := st

		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(gctx, st, s.debugLogging, r.Start, r.End); !ok {
			if s.debugLogging {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to %v", st, reason))
			}
			continue
		}
		if s.debugLogging {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s queried", st))
		}

		g.Go(func() error {
			resp, err := st.LabelNames(gctx, &storepb.LabelNamesRequest{
				PartialResponseDisabled: r.PartialResponseDisabled,
				Start:                   r.Start,
				End:                     r.End,
				Matchers:                r.Matchers,
			})
			if err != nil {
				err = errors.Wrapf(err, "fetch label names from store %s", st)
				if r.PartialResponseDisabled {
					return err
				}

				mtx.Lock()
				warnings = append(warnings, err.Error())
				mtx.Unlock()
				return nil
			}

			mtx.Lock()
			warnings = append(warnings, resp.Warnings...)
			names = append(names, resp.Names)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	_ = level.Debug(s.logger).Log("msg", strings.Join(storeDebugMsgs, ";"))

	result := strutil.MergeUnsortedSlices(names...)
	if r.Limit > 0 && len(result) > int(r.Limit) {
		result = result[:r.Limit]
	}
	return &storepb.LabelNamesResponse{
		Names:    result,
		Warnings: warnings,
	}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *ProxyStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	type storeResult struct {
		values   []string
		warnings []string
	}

	var (
		warnings       []string
		g, gctx        = errgroup.WithContext(ctx)
		storeDebugMsgs []string
		resultsChan    = make(chan storeResult, 1000) // TODO: Adjust buffer size
		resultSet      = make(map[string]struct{})
	)

	ctxWithCancel, cancel := context.WithCancel(gctx)
	defer cancel()

	for _, st := range s.stores() {
		st := st

		storeAddr, isLocalStore := st.Addr()
		storeID := labelpb.PromLabelSetsToString(st.LabelSets())
		if storeID == "" {
			storeID = "Store Gateway"
		}
		storeSpan, storeCtx := tracing.StartSpan(ctx, "proxy.label_values", tracing.Tags{
			"store.id":       storeID,
			"store.addr":     storeAddr,
			"store.is_local": isLocalStore,
		})

		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(storeCtx, st, s.debugLogging, r.Start, r.End); !ok {
			if s.debugLogging {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to %v", st, reason))
			}
			continue
		}
		if s.debugLogging {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s queried", st))
		}

		g.Go(func() error {
			defer storeSpan.Finish()

			resp, err := st.LabelValues(ctxWithCancel, &storepb.LabelValuesRequest{
				Label:                   r.Label,
				PartialResponseDisabled: r.PartialResponseDisabled,
				Start:                   r.Start,
				End:                     r.End,
				Matchers:                r.Matchers,
				Limit:                   r.Limit,
			})
			if err != nil {
				msg := "fetch label values from store %s"
				err = errors.Wrapf(err, msg, st)
				if r.PartialResponseDisabled {
					return err
				}
				resultsChan <- storeResult{
					values:   nil,
					warnings: []string{errors.Wrapf(err, msg, st).Error()},
				}
				return nil
			}

			resultsChan <- storeResult{
				values:   resp.Values,
				warnings: resp.Warnings,
			}
			return nil
		})
	}

	go func() {
		_ = g.Wait() // Ignore error since we handle it via partial response
		close(resultsChan)
	}()

	// Collect results from channel
collectLoop:
	for result := range resultsChan {
		warnings = append(warnings, result.warnings...)
		for _, val := range result.values {
			if _, exists := resultSet[val]; !exists {
				resultSet[val] = struct{}{}
				if r.Limit > 0 && len(resultSet) >= int(r.Limit) {
					cancel() // Cancel remaining goroutines
					break collectLoop
				}
			}
		}
	}

	_ = level.Debug(s.logger).Log("msg", strings.Join(storeDebugMsgs, ";"))
	vals := slices.Collect(maps.Keys(resultSet))
	slices.Sort(vals)
	return &storepb.LabelValuesResponse{
		Values:   vals,
		Warnings: warnings,
	}, nil
}
