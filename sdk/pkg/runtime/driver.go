// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
)

type DriverID struct {
	Name    string
	Version string
}

func (i DriverID) String() string {
	return fmt.Sprintf("%s/%s", i.Name, i.Version)
}

type Driver interface {
	fmt.Stringer
	ID() DriverID
	Connect(ctx context.Context, spec ConnSpec) (Conn, error)
}
