// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
)

const (
	Name       = "Value"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Value valuev1.ValueServer

type ValueProvider interface {
	NewValueV1(primitiveID runtimev1.PrimitiveID) (Value, error)
}

type ConfigurableValueProvider[S any] interface {
	NewValueV1(primitiveID runtimev1.PrimitiveID, spec S) (Value, error)
}
