// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "Counter"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Counter counterv1.CounterServer

type CounterProvider interface {
	NewCounterV1(primitiveID runtimev1.PrimitiveID) (Counter, error)
}

type ConfigurableCounterProvider[S any] interface {
	NewCounterV1(primitiveID runtimev1.PrimitiveID, spec S) (Counter, error)
}
