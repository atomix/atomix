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
	return &raftDriver{
		network: network,
	}
}

type raftDriver struct {
	network network.Driver
}

func (d *raftDriver) Connect(ctx context.Context, config *rsmapiv1.ProtocolConfig) (driver.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, config); err != nil {
		return nil, err
	}
	return conn, nil
}
