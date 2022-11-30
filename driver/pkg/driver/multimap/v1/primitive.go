// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	multimapv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/multimap/v1"
)

type MultiMapProxy multimapv1.MultiMapServer

type MultiMapProvider interface {
	NewMultiMap(spec driver.PrimitiveSpec) (MultiMapProxy, error)
}
