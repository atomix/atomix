// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "Set"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Set setv1.SetServer

type SetProvider interface {
	NewSetV1(primitiveID runtimev1.PrimitiveID) (Set, error)
}

type ConfigurableSetProvider[S any] interface {
	NewSetV1(primitiveID runtimev1.PrimitiveID, spec S) (Set, error)
}
