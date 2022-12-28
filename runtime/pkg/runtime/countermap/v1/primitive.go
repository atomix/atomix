// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	countermapv1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "CounterMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type CounterMap countermapv1.CounterMapServer

type CounterMapProvider interface {
	NewCounterMapV1(primitiveID runtimev1.PrimitiveID) (CounterMap, error)
}

type ConfigurableCounterMapProvider[S any] interface {
	NewCounterMapV1(primitiveID runtimev1.PrimitiveID, spec S) (CounterMap, error)
}
