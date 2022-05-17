// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package codec

import "github.com/golang/protobuf/proto"

func NewProtoCodec[C proto.Message](prototype C) Codec[C] {
	return &protoCodec[C]{
		prototype: prototype,
	}
}

type protoCodec[C proto.Message] struct {
	prototype C
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

var _ Codec[proto.Message] = (*protoCodec[proto.Message])(nil)
