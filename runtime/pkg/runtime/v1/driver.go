// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
)

// Driver is the primary interface for implementing storage drivers
type Driver interface {
	fmt.Stringer
	ID() runtimev1.DriverID
	Connect(ctx context.Context, store runtimev1.Store) (Conn, error)
}

// Conn is a connection to a store
// Implement the Configurator interface to support configuration changes to an existing connection
type Conn interface {
	Closer
}

// Configurator is an interface for supporting configuration changes on an existing Conn
type Configurator interface {
	Configure(ctx context.Context, store runtimev1.Store) error
}

// Closer is an interface for closing connections
type Closer interface {
	Close(ctx context.Context) error
}

type DriverProvider interface {
	LoadDriver(ctx context.Context, driverID runtimev1.DriverID) (Driver, error)
}

func newStaticDriverProvider(drivers ...Driver) DriverProvider {
	driverMap := make(map[runtimev1.DriverID]Driver)
	for _, driver := range drivers {
		driverMap[driver.ID()] = driver
	}
	return &staticDriverProvider{
		drivers: driverMap,
	}
}

type staticDriverProvider struct {
	drivers map[runtimev1.DriverID]Driver
}

func (p *staticDriverProvider) LoadDriver(_ context.Context, driverID runtimev1.DriverID) (Driver, error) {
	driver, ok := p.drivers[driverID]
	if !ok {
		return nil, errors.NewNotFound("driver %s not found", driverID)
	}
	return driver, nil
}

var _ DriverProvider = (*staticDriverProvider)(nil)
