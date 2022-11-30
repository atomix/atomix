// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	setv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/set/v1"
)

type SetProxy setv1.SetServer

type SetProvider interface {
	NewSet(spec driver.PrimitiveSpec) (SetProxy, error)
}
