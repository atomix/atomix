// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/atomix/api/atomix/lock/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type LockProxy lockv1.LockServer

type LockProvider interface {
	NewLock(spec driver.PrimitiveSpec) (LockProxy, error)
}
