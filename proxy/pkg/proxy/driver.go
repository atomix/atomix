// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
	"os"
	"path/filepath"
	"plugin"
)

const driverSymName = "Plugin"

func NewDriverProvider(path string) runtime.DriverProvider {
	return &pluginDriverProvider{
		path: path,
	}
}

type pluginDriverProvider struct {
	path string
}

func (p *pluginDriverProvider) LoadDriver(_ context.Context, driverID runtimev1.DriverID) (driver.Driver, error) {
	path := filepath.Join(p.path, fmt.Sprintf("%s@%s.so", driverID.Name, driverID.APIVersion))
	driverPlugin, err := plugin.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.NewNotFound(err.Error())
		}
		return nil, errors.NewUnknown(err.Error())
	}
	driverSym, err := driverPlugin.Lookup(driverSymName)
	if err != nil {
		return nil, errors.NewNotFound(err.Error())
	}
	return *driverSym.(*driver.Driver), nil
}

var _ runtime.DriverProvider = (*pluginDriverProvider)(nil)
