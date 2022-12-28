// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "List"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type List listv1.ListServer

type ListProvider interface {
	NewListV1(primitiveID runtimev1.PrimitiveID) (List, error)
}

type ConfigurableListProvider[S any] interface {
	NewListV1(primitiveID runtimev1.PrimitiveID, spec S) (List, error)
}
