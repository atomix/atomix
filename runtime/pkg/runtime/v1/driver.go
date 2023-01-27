// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
)

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
