// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/atomix/api/atomix/counter/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type CounterProxy counterv1.CounterServer

type CounterProvider interface {
	NewCounter(spec driver.PrimitiveSpec) (CounterProxy, error)
}
