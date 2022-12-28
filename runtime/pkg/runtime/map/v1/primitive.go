// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "Map"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Map mapv1.MapServer

type MapProvider interface {
	NewMapV1(primitiveID runtimev1.PrimitiveID) (Map, error)
}

type ConfigurableMapProvider[S any] interface {
	NewMapV1(primitiveID runtimev1.PrimitiveID, spec S) (Map, error)
}
