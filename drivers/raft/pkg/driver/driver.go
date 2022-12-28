// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	runtimeapiv1 "github.com/atomix/atomix/api/runtime/v1"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	runtimev1 "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var driverID = runtimeapiv1.DriverID{
	Name:       "Raft",
	APIVersion: "v1",
}

func New(network network.Driver) runtimev1.Driver {
	return &multiRaftDriver{
		network: network,
	}
}

type multiRaftDriver struct {
	network network.Driver
}

func (d *multiRaftDriver) ID() runtimeapiv1.DriverID {
	return driverID
}

func (d *multiRaftDriver) Connect(ctx context.Context, config *rsmapiv1.ProtocolConfig) (runtimev1.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, config); err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *multiRaftDriver) String() string {
	return fmt.Sprintf("%s/%s", driverID.Name, driverID.APIVersion)
}
