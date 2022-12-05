// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/atomix/api/atomix/map/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type MapProxy mapv1.MapServer

type MapProvider interface {
	NewMap(spec driver.PrimitiveSpec) (MapProxy, error)
}
