// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/pkg/driver"
)

type SetProxy SetServer

type SetProvider interface {
	NewSet(spec driver.PrimitiveSpec) (SetProxy, error)
}
