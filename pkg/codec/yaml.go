// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"gopkg.in/yaml.v3"
)

func NewYAMLCodec[C struct{}]() Codec[C] {
	return &yamlCodec[C]{}
}

type yamlCodec[C struct{}] struct{}

func (c *yamlCodec[C]) Encode(config C) ([]byte, error) {
	return yaml.Marshal(&config)
}

func (c *yamlCodec[C]) Decode(bytes []byte) (C, error) {
	var config C
	if err := yaml.Unmarshal(bytes, &config); err != nil {
		return config, err
	}
	return config, nil
}

var _ Codec[struct{}] = (*yamlCodec[struct{}])(nil)
