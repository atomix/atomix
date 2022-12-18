// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/pkg/driver"
)

type ListProxy ListServer

type ListProvider interface {
	NewList(spec driver.PrimitiveSpec) (ListProxy, error)
}
