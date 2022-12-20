// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var driverID = runtimev1.DriverID{
	Name:    "PodMemory",
	Version: "v2beta1",
}

func New(network network.Driver) runtime.Driver {
	return &sharedMemoryDriver{
		network: network,
	}
}

type sharedMemoryDriver struct {
	network network.Driver
}

func (d *sharedMemoryDriver) ID() runtimev1.DriverID {
	return driverID
}

func (d *sharedMemoryDriver) Connect(ctx context.Context, spec runtimev1.ConnSpec) (runtime.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, spec); err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *sharedMemoryDriver) String() string {
	return fmt.Sprintf("%s/%s", driverID.Name, driverID.Version)
}
