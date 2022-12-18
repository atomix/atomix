// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/atomix/api/pkg/value/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type ValueProxy valuev1.ValueServer

type ValueProvider interface {
	NewValue(spec driver.PrimitiveSpec) (ValueProxy, error)
}
