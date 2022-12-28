// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type IndexedMap indexedmapv1.IndexedMapServer

type IndexedMapProvider interface {
	NewIndexedMapV1(primitiveID runtimev1.PrimitiveID) (IndexedMap, error)
}

type ConfigurableIndexedMapProvider[S any] interface {
	NewIndexedMapV1(primitiveID runtimev1.PrimitiveID, spec S) (IndexedMap, error)
}
