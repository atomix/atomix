// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/atomix/api/atomix/list/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
)

type ListProxy listv1.ListServer

type ListProvider interface {
	NewList(spec driver.PrimitiveSpec) (ListProxy, error)
}
