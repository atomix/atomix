// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	valuev1 "github.com/atomix/atomix/runtime/api/atomix/runtime/value/v1"
)

type ValueProxy valuev1.ValueServer

type ValueProvider interface {
	NewValue(spec driver.PrimitiveSpec) (ValueProxy, error)
}
