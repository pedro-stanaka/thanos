// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bytes"
	"errors"
	"io"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/klauspost/compress/zstd"
)

type PackedCodec struct {
	*capnp.Encoder
	*capnp.Decoder
	io.Closer
}

func NewPackedCodec(rwc io.ReadWriteCloser) (rpc.Codec, error) {
	return &PackedCodec{
		Encoder: capnp.NewPackedEncoder(rwc),
		Decoder: capnp.NewPackedDecoder(rwc),
		Closer:  rwc,
	}, nil
}

type ZSTDCodec struct {
	rwc io.ReadWriteCloser

	encoder    *capnp.Encoder
	decoder    *capnp.Decoder
	zstdWriter *zstd.Encoder
	encoded    *bytes.Buffer
	compressed *bytes.Buffer
}

func NewZSTDCodec(rwc io.ReadWriteCloser) (rpc.Codec, error) {
	zstdWriter, err := zstd.NewWriter(rwc, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	zstdReader, err := zstd.NewReader(rwc, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	var (
		writeBuffer      = bytes.NewBuffer(nil)
		compressedBuffer = bytes.NewBuffer(nil)
	)
	return &ZSTDCodec{
		zstdWriter: zstdWriter,
		encoded:    writeBuffer,
		compressed: compressedBuffer,

		encoder: capnp.NewEncoder(writeBuffer),
		decoder: capnp.NewDecoder(zstdReader),
		rwc:     rwc,
	}, nil
}

func (z *ZSTDCodec) Encode(message *capnp.Message) error {
	z.encoded.Reset()
	z.compressed.Reset()
	if err := z.encoder.Encode(message); err != nil {
		return err
	}
	b := z.zstdWriter.EncodeAll(z.encoded.Bytes(), z.compressed.Bytes())
	_, err := z.rwc.Write(b)
	return err
}

func (z *ZSTDCodec) Decode() (*capnp.Message, error) {
	return z.decoder.Decode()
}

func (z *ZSTDCodec) Close() error {
	return errors.Join(z.zstdWriter.Close(), z.rwc.Close())
}
