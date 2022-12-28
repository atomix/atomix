// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	multimapv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "MultiMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type MultiMap multimapv1.MultiMapServer

type MultiMapProvider interface {
	NewMultiMapV1(primitiveID runtimev1.PrimitiveID) (MultiMap, error)
}

type ConfigurableMultiMapProvider[S any] interface {
	NewMultiMapV1(primitiveID runtimev1.PrimitiveID, spec S) (MultiMap, error)
}
