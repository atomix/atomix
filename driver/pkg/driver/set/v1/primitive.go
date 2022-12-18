// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/atomix/api/pkg/set/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type SetProxy setv1.SetServer

type SetProvider interface {
	NewSet(spec driver.PrimitiveSpec) (SetProxy, error)
}
