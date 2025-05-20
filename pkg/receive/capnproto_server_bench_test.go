// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func BenchmarkCapNProtoServer_SingleConcurrentClient(b *testing.B) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1},
					{Timestamp: 2, Value: 2},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "thanos"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 3, Value: 3},
					{Timestamp: 4, Value: 4},
				},
			},
		},
	}

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener, zstdListener = bufconn.Listen(1024), bufconn.Listen(1024)
		handler                = NewCapNProtoHandler(log.NewNopLogger(), writer)
	)
	protocols := map[string]tuple[writecapnp.Dialer, writecapnp.NewCodecFunc]{
		"packed": {listener, writecapnp.NewPackedCodec},
		"zstd":   {zstdListener, writecapnp.NewZSTDCodec},
	}
	for name, protocol := range protocols {
		srv := NewCapNProtoServer(listener, zstdListener, handler, log.NewNopLogger())
		go func() {
			_ = srv.ListenAndServe()
		}()
		b.Cleanup(srv.Shutdown)
		b.Run(name, func(b *testing.B) {
			const numIterations = 10000
			var totalWrites float64
			b.ResetTimer()
			b.ReportAllocs()
			client := writecapnp.NewRemoteWriteClient(protocol.A, protocol.B, log.NewLogfmtLogger(os.Stdout))
			for i := 0; i < b.N; i++ {
				for j := 0; j < numIterations; j++ {
					_, err := client.RemoteWrite(context.Background(), &wreq)
					require.NoError(b, err)
				}
				totalWrites += numIterations
			}
			require.NoError(b, client.Close())
			b.ReportMetric(totalWrites, "total_writes")
		})
	}
	require.NoError(b, errors.Join(listener.Close(), zstdListener.Close()))
}
