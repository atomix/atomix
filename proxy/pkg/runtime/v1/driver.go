// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/gogo/protobuf/types"
)

func newDriverAdapter(driver driver.Driver) driver.Driver {
	return &driverAdapter{
		driver: driver,
	}
}

type driverAdapter struct {
	driver driver.Driver
}

func (d *driverAdapter) Connect(ctx context.Context, spec *types.Any) (driver.Conn, error) {
	if connector, ok := d.driver.(driver.Connector[*types.Any]); ok {
		return connector.Connect(ctx, spec)
	}
	conn, err := connect(ctx, d.driver, spec)
	if err != nil {
		return nil, err
	}
	return newConnAdapter(conn), nil
}

func newConnAdapter(conn driver.Conn) driver.Conn {
	return &connAdapter{
		conn: conn,
	}
}

type connAdapter struct {
	conn driver.Conn
}

func (c *connAdapter) Configure(ctx context.Context, spec *types.Any) error {
	if configurator, ok := c.conn.(driver.Configurator[*types.Any]); ok {
		return configurator.Configure(ctx, spec)
	}
	return configure(ctx, c.conn, spec)
}

func (c *connAdapter) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

type DriverProvider interface {
	LoadDriver(ctx context.Context, driverID runtimev1.DriverID) (driver.Driver, error)
}

func newStaticDriverProvider(drivers map[runtimev1.DriverID]driver.Driver) DriverProvider {
	return &staticDriverProvider{
		drivers: drivers,
	}
}

type staticDriverProvider struct {
	drivers map[runtimev1.DriverID]driver.Driver
}

func (p *staticDriverProvider) LoadDriver(_ context.Context, driverID runtimev1.DriverID) (driver.Driver, error) {
	driver, ok := p.drivers[driverID]
	if !ok {
		return nil, errors.NewNotFound("driver %s not found", driverID)
	}
	return driver, nil
}

var _ DriverProvider = (*staticDriverProvider)(nil)
