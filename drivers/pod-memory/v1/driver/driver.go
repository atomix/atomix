// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
)

func New(network network.Driver) driver.Driver {
	return &podMemoryDriver{
		network: network,
	}
}

type podMemoryDriver struct {
	network network.Driver
}

func (d *podMemoryDriver) Connect(ctx context.Context, spec rsmapiv1.ProtocolConfig) (driver.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, spec); err != nil {
		return nil, err
	}
	return conn, nil
}
