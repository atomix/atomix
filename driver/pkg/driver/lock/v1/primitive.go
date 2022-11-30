// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	lockv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/lock/v1"
)

type LockProxy lockv1.LockServer

type LockProvider interface {
	NewLock(spec driver.PrimitiveSpec) (LockProxy, error)
}
