// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	indexedmapv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/indexedmap/v1"
)

type IndexedMapProxy indexedmapv1.IndexedMapServer

type IndexedMapProvider interface {
	NewIndexedMap(spec driver.PrimitiveSpec) (IndexedMapProxy, error)
}
