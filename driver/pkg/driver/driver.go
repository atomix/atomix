// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
)

type Driver interface {
	fmt.Stringer
	Name() string
	Version() string
	Connect(ctx context.Context, spec ConnSpec) (Conn, error)
}
