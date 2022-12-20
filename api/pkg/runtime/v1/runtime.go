// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

func (m *PrimitiveSpec) UnmarshalConfig(config proto.Message) error {
	return jsonpb.UnmarshalString(string(m.Config), config)
}

func (m *ConnSpec) UnmarshalConfig(config proto.Message) error {
	return jsonpb.UnmarshalString(string(m.Config), config)
}
