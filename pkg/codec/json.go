// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"encoding/json"
)

func NewJSONCodec[C struct{}]() Codec[C] {
	return &jsonCodec[C]{}
}

type jsonCodec[C struct{}] struct{}

func (c *jsonCodec[C]) Encode(config C) ([]byte, error) {
	return json.Marshal(&config)
}

func (c *jsonCodec[C]) Decode(bytes []byte) (C, error) {
	var config C
	if err := json.Unmarshal(bytes, &config); err != nil {
		return config, err
	}
	return config, nil
}

var _ Codec[struct{}] = (*jsonCodec[struct{}])(nil)
