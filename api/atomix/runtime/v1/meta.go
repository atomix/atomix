// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import "github.com/gogo/protobuf/proto"

type Object interface {
	proto.Message
	GetMeta() ObjectMeta
	SetMeta(ObjectMeta)
}

type ObjectRevision uint64
