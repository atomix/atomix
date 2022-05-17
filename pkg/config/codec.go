// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/golang/protobuf/proto"

type Codec[C Config] interface {
	Encode(config C) ([]byte, error)
	Decode(bytes []byte) (C, error)
}

func NewCodec[C Config](prototype Config) Codec[C] {
	return &protoCodec[C]{
		prototype: prototype,
	}
}

type protoCodec[C Config] struct {
	prototype Config
}

func (c *protoCodec[C]) Encode(config C) ([]byte, error) {
	return proto.Marshal(config)
}

func (c *protoCodec[C]) Decode(bytes []byte) (C, error) {
	config := proto.Clone(c.prototype).(C)
	if err := proto.Unmarshal(bytes, config); err != nil {
		return config, err
	}
	return config, nil
}

var _ Codec[Config] = (*protoCodec[Config])(nil)
