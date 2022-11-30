// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	countermapv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/countermap/v1"
)

type CounterMapProxy countermapv1.CounterMapServer

type CounterMapProvider interface {
	NewCounterMap(spec driver.PrimitiveSpec) (CounterMapProxy, error)
}
