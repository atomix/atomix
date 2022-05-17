// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package codec

type Provider[C any] func() C

type Codec[C any] interface {
	Encode(config C) ([]byte, error)
	Decode(bytes []byte) (C, error)
}
