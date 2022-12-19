// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/runtime"
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

func (p *pluginDriverProvider) LoadDriver(_ context.Context, name, version string) (runtime.Driver, error) {
	path := filepath.Join(p.path, fmt.Sprintf("%s@%s.so", name, version))
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
	driver := *driverSym.(*runtime.Driver)
	return driver, nil
}

var _ runtime.DriverProvider = (*pluginDriverProvider)(nil)
