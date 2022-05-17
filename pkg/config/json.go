// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
)

func NewJSONCodec[C any](provider Provider[C]) Codec[C] {
	return &jsonCodec[C]{
		provider: provider,
	}
}

type jsonCodec[C any] struct {
	provider Provider[C]
}

func (c *jsonCodec[C]) Encode(config C) ([]byte, error) {
	return json.Marshal(&config)
}

func (c *jsonCodec[C]) Decode(bytes []byte) (C, error) {
	config := c.provider()
	if err := json.Unmarshal(bytes, &config); err != nil {
		return config, err
	}
	return config, nil
}

var _ Codec[any] = (*jsonCodec[any])(nil)
