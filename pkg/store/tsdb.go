// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"hash"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const RemoteReadFrameLimit = 1048576

type TSDBReader interface {
	storage.ChunkQueryable
	StartTime() (int64, error)
}

// TSDBStore implements the store API against a local TSDB instance.
// It attaches the provided external labels to all results. It only responds with raw data
// and does not support downsampling.
type TSDBStore struct {
	logger    log.Logger
	db        TSDBReader
	component component.StoreAPI

	extLset        labels.Labels
	extLsetString  string
	extLabelsSlice []labels.Label

	buffers          sync.Pool
	maxBytesPerFrame int

	mtx sync.RWMutex
}

func RegisterWritableStoreServer(storeSrv storepb.WriteableStoreServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		storepb.RegisterWriteableStoreServer(s, storeSrv)
	}
}

// ReadWriteTSDBStore is a TSDBStore that can also be written to.
type ReadWriteTSDBStore struct {
	storepb.StoreServer
	storepb.WriteableStoreServer
}

// NewTSDBStore creates a new TSDBStore.
// NOTE: Given lset has to be sorted.
func NewTSDBStore(logger log.Logger, db TSDBReader, component component.StoreAPI, extLset labels.Labels) *TSDBStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &TSDBStore{
		logger:           logger,
		db:               db,
		component:        component,
		extLset:          extLset,
		extLsetString:    extLset.String(),
		extLabelsSlice:   labelsToSlice(extLset),
		maxBytesPerFrame: RemoteReadFrameLimit,
		buffers: sync.Pool{New: func() interface{} {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},
	}
}

func (s *TSDBStore) SetExtLset(extLset labels.Labels) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.extLset = extLset
	s.extLsetString = extLset.String()
	s.extLabelsSlice = labelsToSlice(extLset)
}

func (s *TSDBStore) getExtLset() labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.extLset
}

func (s *TSDBStore) getExtLabelsSlice() []labels.Label {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.extLabelsSlice
}

func (s *TSDBStore) LabelSet() []labels.Labels {
	return []labels.Labels{
		s.getExtLset(),
	}
}

func (s *TSDBStore) String() string {
	return s.extLsetString
}

func (p *TSDBStore) TSDBInfos() []infopb.TSDBInfo {
	labels := p.LabelSet()
	if len(labels) == 0 {
		return []infopb.TSDBInfo{}
	}

	mint, maxt := p.TimeRange()
	return []infopb.TSDBInfo{
		{
			Labels: labelpb.ZLabelSet{
				Labels: labelpb.ZLabelsFromPromLabels(labels[0]),
			},
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

func (s *TSDBStore) TimeRange() (int64, int64) {
	var minTime int64 = math.MinInt64
	startTime, err := s.db.StartTime()
	if err == nil {
		// Since we always use tsdb.DB implementation,
		// StartTime should never return error.
		minTime = startTime
	}

	return minTime, math.MaxInt64
}

// CloseDelegator allows to delegate close (releasing resources used by request to the server).
// This is useful when we invoke StoreAPI within another StoreAPI and results are ephemeral until copied.
type CloseDelegator interface {
	Delegate(io.Closer)
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *TSDBStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return nil
	}

	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}

	q, err := s.db.ChunkQuerier(r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if cd, ok := srv.(CloseDelegator); ok {
		cd.Delegate(q)
	} else {
		defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb chunk querier series")
	}

	hints := &storage.SelectHints{
		Start: r.MinTime,
		End:   r.MaxTime,
		// The promql-engine will already skip samples that are outside the time range.
		// No need to trim and re-encode chunks just because of that.
		DisableTrimming: true,
		Limit:           int(r.Limit),
	}
	set := q.Select(srv.Context(), true, hints, matchers...)

	shardMatcher := r.ShardInfo.Matcher(&s.buffers)
	defer shardMatcher.Close()
	hasher := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(hasher)

	var (
		withoutLabels = make(map[string]struct{})
		lblsScratch   []labelpb.ZLabel
	)
	for _, lbl := range r.WithoutReplicaLabels {
		withoutLabels[lbl] = struct{}{}
	}
	// Stream at most one series per frame; series may be split over multiple frames according to maxBytesInFrame.
	for set.Next() {
		series := set.At()

		lbls := buildSeriesLabels(
			lblsScratch,
			series.Labels(),
			s.getExtLabelsSlice(),
			withoutLabels,
		)
		if !shardMatcher.MatchesZLabels(lbls) {
			continue
		}

		lblsCopy := make([]labelpb.ZLabel, len(lbls))
		copy(lblsCopy, lbls)
		storeSeries := storepb.Series{Labels: lblsCopy}
		if r.SkipChunks {
			if err := srv.Send(storepb.NewSeriesResponse(&storeSeries)); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			continue
		}

		bytesLeftForChunks := s.maxBytesPerFrame
		for _, lbl := range storeSeries.Labels {
			bytesLeftForChunks -= lbl.Size()
		}
		frameBytesLeft := bytesLeftForChunks

		seriesChunks := []storepb.AggrChunk{}
		chIter := series.Iterator(nil)
		isNext := chIter.Next()
		for isNext {
			chk := chIter.At()
			if chk.Chunk == nil {
				return status.Errorf(codes.Internal, "TSDBStore: found not populated chunk returned by SeriesSet at ref: %v", chk.Ref)
			}

			chunkBytes := make([]byte, len(chk.Chunk.Bytes()))
			copy(chunkBytes, chk.Chunk.Bytes())
			c := storepb.AggrChunk{
				MinTime: chk.MinTime,
				MaxTime: chk.MaxTime,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_Encoding(chk.Chunk.Encoding() - 1), // Proto chunk encoding is one off to TSDB one.
					Data: chunkBytes,
					Hash: hashChunk(hasher, chunkBytes, enableChunkHashCalculation),
				},
			}
			frameBytesLeft -= c.Size()
			seriesChunks = append(seriesChunks, c)

			// We are fine with minor inaccuracy of max bytes per frame. The inaccuracy will be max of full chunk size.
			isNext = chIter.Next()
			if frameBytesLeft > 0 && isNext {
				continue
			}
			if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: storeSeries.Labels, Chunks: seriesChunks})); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}

			if isNext {
				frameBytesLeft = bytesLeftForChunks
				seriesChunks = make([]storepb.AggrChunk, 0, len(seriesChunks))
			}
		}
		if err := chIter.Err(); err != nil {
			return status.Error(codes.Internal, errors.Wrap(err, "chunk iter").Error())
		}

	}
	if err := set.Err(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	for _, w := range set.Warnings() {
		if err := srv.Send(storepb.NewWarnSeriesResponse(w)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

func buildSeriesLabels(lbls []labelpb.ZLabel, seriesLabels labels.Labels, extLabels []labels.Label, withoutLabels map[string]struct{}) []labelpb.ZLabel {
	lbls = lbls[:0]
	iExt := 0
	seriesLabels.Range(func(intLbl labels.Label) {
	extLoop:
		for iExt < len(extLabels) {
			cmp := strings.Compare(extLabels[iExt].Name, intLbl.Name)
			if cmp < 1 {
				extLbl := extLabels[iExt]
				if _, ok := withoutLabels[extLbl.Name]; !ok {
					lbls = append(lbls, labelpb.ZLabel{Name: extLbl.Name, Value: extLbl.Value})
				}
				iExt++
			}
			// If the current internal label is identical to the external label, move over to
			// the next internal label to avoid duplicates.
			// Otherwise, move over to adding internal labels.
			switch cmp {
			case 0:
				return
			case 1:
				break extLoop
			}
		}
		if _, ok := withoutLabels[intLbl.Name]; !ok {
			lbls = append(lbls, labelpb.ZLabel{Name: intLbl.Name, Value: intLbl.Value})
		}
	})
	for _, l := range extLabels[iExt:] {
		if _, ok := withoutLabels[l.Name]; !ok {
			lbls = append(lbls, labelpb.ZLabel{Name: l.Name, Value: l.Value})
		}
	}
	return lbls
}

// GuaranteedMinTime returns the minimum timestamp in milliseconds that will always be present in a TSDB.
func GuaranteedMinTime(now, mint, retentionDuration, minBlockDuration int64) int64 {
	if mint == UninitializedTSDBTime {
		return UninitializedTSDBTime
	}

	estimatedRetentionTime := time.UnixMilli(now - retentionDuration)
	blockAlignedRetention := estimatedRetentionTime.
		Truncate(time.Duration(minBlockDuration) * time.Millisecond).
		UnixMilli()

	// The first few samples in a block are sometimes not wall-clock aligned.
	// We add a small offset proportional to the block size to make sure we skip
	// the first few minutes in a TSDB.
	return blockAlignedRetention + minBlockDuration/8
}

// LabelNames returns all known label names constrained with the given matchers.
func (s *TSDBStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return &storepb.LabelNamesResponse{Names: nil}, nil
	}

	q, err := s.db.ChunkQuerier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label names")

	hints := &storage.LabelHints{
		Limit: int(r.Limit),
	}
	res, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if len(res) > 0 {
		s.getExtLset().Range(func(l labels.Label) {
			res = append(res, l.Name)
		})
		sort.Strings(res)
	}

	// Label values can come from a postings table of a memory-mapped block which can be deleted during
	// head compaction. Since we close the block querier before we return from the function,
	// we need to copy label values to make sure the client still has access to the data when
	// a block is deleted.
	values := make([]string, len(res))
	for i := range res {
		values[i] = strings.Clone(res[i])
	}

	return &storepb.LabelNamesResponse{Names: values}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *TSDBStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	if r.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}

	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{Values: nil}, nil
	}

	q, err := s.db.ChunkQuerier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label values")

	// If we request label values for an external label while selecting an additional matcher for other label values
	if val := s.getExtLset().Get(r.Label); val != "" {
		if len(matchers) == 0 {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}

		hints := &storage.SelectHints{
			Start: r.Start,
			End:   r.End,
			Func:  "series",
			Limit: int(r.Limit),
		}
		set := q.Select(ctx, false, hints, matchers...)

		for set.Next() {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}
		return &storepb.LabelValuesResponse{}, nil
	}

	hints := &storage.LabelHints{
		Limit: int(r.Limit),
	}
	res, _, err := q.LabelValues(ctx, r.Label, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if hints.Limit > 0 && len(res) > hints.Limit {
		res = res[:hints.Limit]
	}

	// Label values can come from a postings table of a memory-mapped block which can be deleted during
	// head compaction. Since we close the block querier before we return from the function,
	// we need to copy label values to make sure the client still has access to the data when
	// a block is deleted.
	values := make([]string, len(res))
	for i := range res {
		values[i] = strings.Clone(res[i])
	}

	return &storepb.LabelValuesResponse{Values: values}, nil
}

func labelsToSlice(lset labels.Labels) []labels.Label {
	r := make([]labels.Label, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		r = append(r, l)
	})
	return r
}
