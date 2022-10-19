// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

type Codec[I, O any] interface {
	EncodeInput(I) ([]byte, error)
	DecodeOutput([]byte) (O, error)
}

type DecodeFunc[T any] func([]byte) (T, error)

type EncodeFunc[T any] func(T) ([]byte, error)

func NewCodec[I, O any](encoder EncodeFunc[I], decoder DecodeFunc[O]) Codec[I, O] {
	return &codec[I, O]{
		encoder: encoder,
		decoder: decoder,
	}
}

type codec[I, O any] struct {
	encoder EncodeFunc[I]
	decoder DecodeFunc[O]
}

func (c *codec[I, O]) EncodeInput(input I) ([]byte, error) {
	return c.encoder(input)
}

func (c *codec[I, O]) DecodeOutput(bytes []byte) (O, error) {
	return c.decoder(bytes)
}
