// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import "github.com/gogo/protobuf/proto"

type ObjectID interface {
	proto.Message
	Equal(interface{}) bool
}

type Object[I ObjectID] interface {
	proto.Message
	GetID() I
	SetID(I)
	GetVersion() ObjectVersion
	SetVersion(ObjectVersion)
}

type ObjectVersion uint64
