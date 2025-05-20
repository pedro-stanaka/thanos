// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestCodec(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	codec, err := writecapnp.NewZSTDCodec(nopCloser{buffer})
	require.NoError(t, err)

	arena := capnp.SingleSegment(nil)
	defer arena.Release()
	msg, err := makeMessage(arena, []prompb.TimeSeries{{
		Labels:  []labelpb.ZLabel{{Name: "test-name", Value: "test-val"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: 2}},
	}})
	require.NoError(t, err)

	for range 100 {
		require.NoError(t, codec.Encode(msg))
	}

	decoder, err := writecapnp.NewZSTDCodec(nopCloser{bytes.NewBuffer(buffer.Bytes())})
	require.NoError(t, err)
	for range 100 {
		recv, err := decoder.Decode()
		require.NoError(t, err)
		root, err := writecapnp.ReadRootWriteRequest(recv)
		require.NoError(t, err)
		req, err := writecapnp.NewRequest(root)
		require.NoError(t, err)
		series := writecapnp.Series{}
		require.True(t, req.Next())
		require.NoError(t, req.At(&series))

		require.Equal(t, "test-name", series.Labels[0].Name)
		require.Equal(t, "test-val", series.Labels[0].Value)

		require.False(t, req.Next())
	}
}

func BenchmarkCodec(b *testing.B) {
	arena := capnp.SingleSegment(nil)
	defer arena.Release()
	msg, err := makeMessage(arena, makeTimeSeries(10, 10, 10))
	require.NoError(b, err)

	b.Run("default_codec", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			buffer := bytes.NewBuffer(nil)
			buffer.Reset()
			defaultCodec := capnp.NewEncoder(buffer)
			require.NoError(b, defaultCodec.Encode(msg))
			b.ReportMetric(float64(buffer.Len()), "default")
		}
	})
	b.Run("zstd_codec", func(b *testing.B) {
		b.ReportAllocs()
		buffer := bytes.NewBuffer(nil)
		zstdCodec, err := writecapnp.NewZSTDCodec(nopCloser{buffer})
		require.NoError(b, err)
		for range b.N {
			buffer.Reset()
			require.NoError(b, zstdCodec.Encode(msg))
			b.ReportMetric(float64(buffer.Len()), "compressed_size")
		}
	})
}

func makeMessage(arena *capnp.SingleSegmentArena, ts []prompb.TimeSeries) (*capnp.Message, error) {
	msg, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return nil, err
	}
	wr, err := writecapnp.NewRootWriteRequest(seg)
	if err != nil {
		return nil, err
	}
	if err := writecapnp.BuildInto(wr, "test", ts); err != nil {
		return nil, err
	}
	return msg, nil
}

type nopCloser struct {
	*bytes.Buffer
}

func (c nopCloser) Read(b []byte) (int, error) {
	n, err := c.Buffer.Read(b)
	fmt.Println(err)
	return n, err
}

func (c nopCloser) Close() error {
	fmt.Println(c.Buffer.String())
	return nil
}

func makeTimeSeries(numSeries int, numClusters int, numPods int) []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, 0, numSeries*numClusters*numPods)
	for i := 0; i < numSeries; i++ {
		for j := 0; j < numClusters; j++ {
			for k := 0; k < numPods; k++ {
				series = append(series, prompb.TimeSeries{
					Labels: []labelpb.ZLabel{{
						Name:  "cluster",
						Value: strconv.Itoa(j),
					}, {
						Name:  "pod",
						Value: strconv.Itoa(k),
					}, {
						Name:  "series",
						Value: strconv.Itoa(i),
					}},
					Samples: []prompb.Sample{
						{Value: 1, Timestamp: 2},
					},
				})
			}
		}
	}
	return series
}
