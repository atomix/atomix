// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Lock lockv1.LockServer

type LockProvider interface {
	NewLockV1(primitiveID runtimev1.PrimitiveID) (Lock, error)
}

type ConfigurableLockProvider[S any] interface {
	NewLockV1(primitiveID runtimev1.PrimitiveID, spec S) (Lock, error)
}
