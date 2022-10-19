// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

const truncLen = 200

type Codec[I, O any] interface {
	DecodeInput([]byte) (I, error)
	EncodeOutput(O) ([]byte, error)
}

type AnyCodec Codec[any, any]

type EncodeFunc[T any] func(T) ([]byte, error)

type DecodeFunc[T any] func([]byte) (T, error)

func NewCodec[I, O any](decoder DecodeFunc[I], encoder EncodeFunc[O]) Codec[I, O] {
	return &codec[I, O]{
		encoder: encoder,
		decoder: decoder,
	}
}

type codec[I, O any] struct {
	decoder DecodeFunc[I]
	encoder EncodeFunc[O]
}

func (c *codec[I, O]) DecodeInput(bytes []byte) (I, error) {
	return c.decoder(bytes)
}

func (c *codec[I, O]) EncodeOutput(output O) ([]byte, error) {
	return c.encoder(output)
}
