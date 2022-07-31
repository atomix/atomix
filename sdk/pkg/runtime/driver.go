// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
)

type DriverID struct {
	Name    string
	Version string
}

func (i DriverID) String() string {
	return fmt.Sprintf("%s/%s", i.Name, i.Version)
}

type Driver interface {
	fmt.Stringer
	ID() DriverID
	Connect(ctx context.Context, config []byte) (Conn, error)
}

func NewDriver[C any](name, version string, connector Connector[C]) Driver {
	return &configurableDriver[C]{
		id: DriverID{
			Name:    name,
			Version: version,
		},
		connector: connector,
	}
}

type configurableDriver[C any] struct {
	id        DriverID
	connector Connector[C]
}

func (d *configurableDriver[C]) ID() DriverID {
	return d.id
}

func (d *configurableDriver[C]) Connect(ctx context.Context, data []byte) (Conn, error) {
	var config C
	if data != nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}
	return d.connector(ctx, config)
}

func (d *configurableDriver[C]) String() string {
	return d.id.String()
}

var _ Driver = (*configurableDriver[any])(nil)
