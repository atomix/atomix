// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
)

// Driver is the primary interface for implementing storage drivers
type Driver interface {
	fmt.Stringer
	Name() string
	Version() string
	Connect(ctx context.Context, config []byte) (Conn, error)
}

// Conn is a connection to a store
// Implement the Configurator interface to support configuration changes to an existing connection
type Conn interface {
	Closer
}

// Configurator is an interface for supporting configuration changes on an existing Conn
type Configurator interface {
	Configure(ctx context.Context, config []byte) error
}

// Closer is an interface for closing connections
type Closer interface {
	Close(ctx context.Context) error
}

type DriverProvider interface {
	LoadDriver(ctx context.Context, name, version string) (Driver, error)
}

func newStaticDriverProvider(drivers ...Driver) DriverProvider {
	driverMap := make(map[string]map[string]Driver)
	for _, driver := range drivers {
		versionMap, ok := driverMap[driver.Name()]
		if !ok {
			versionMap = make(map[string]Driver)
			driverMap[driver.Name()] = versionMap
		}
		versionMap[driver.Version()] = driver
	}
	return &staticDriverProvider{
		drivers: driverMap,
	}
}

type staticDriverProvider struct {
	drivers map[string]map[string]Driver
}

func (p *staticDriverProvider) LoadDriver(_ context.Context, name, version string) (Driver, error) {
	versions, ok := p.drivers[name]
	if !ok {
		return nil, errors.NewNotFound("driver %s not found", name)
	}
	driver, ok := versions[version]
	if !ok {
		return nil, errors.NewNotFound("driver %s version %s not found", name, version)
	}
	return driver, nil
}

var _ DriverProvider = (*staticDriverProvider)(nil)
