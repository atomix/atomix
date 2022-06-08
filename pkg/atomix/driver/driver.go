// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/atomix/runtime/pkg/atomix/logging"
)

var log = logging.GetLogger()

type Driver interface {
	fmt.Stringer
	Name() string
	Version() string
	Connect(ctx context.Context, config []byte) (Conn, error)
}

func New[C any](name, version string, connector Connector[C]) Driver {
	return &configurableDriver[C]{
		name:      name,
		version:   version,
		connector: connector,
	}
}

type configurableDriver[C any] struct {
	name      string
	version   string
	connector Connector[C]
}

func (d *configurableDriver[C]) Name() string {
	return d.name
}

func (d *configurableDriver[C]) Version() string {
	return d.version
}

func (d *configurableDriver[C]) Connect(ctx context.Context, data []byte) (Conn, error) {
	var config C
	if data != nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}
	conn, err := d.connector(ctx, config)
	if err != nil {
		return nil, err
	}
	return newConfigurableConn[C](d, conn), nil
}

func (d *configurableDriver[C]) String() string {
	return fmt.Sprintf("%s/%s", d.name, d.version)
}

var _ Driver = (*configurableDriver[any])(nil)
