// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
	"github.com/atomix/runtime/pkg/logging"
)

var log = logging.GetLogger()

type Driver interface {
	Connect(ctx context.Context, config []byte) (Conn, error)
}

func New[C any](connector Connector[C]) Driver {
	return &configurableDriver[C]{
		connector: connector,
	}
}

type configurableDriver[C any] struct {
	connector Connector[C]
}

func (d *configurableDriver[C]) Connect(ctx context.Context, bytes []byte) (Conn, error) {
	var config C
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}
	conn, err := d.connector(ctx, config)
	if err != nil {
		return nil, err
	}
	return newConfigurableConn[C](conn), nil
}

var _ Driver = (*configurableDriver[any])(nil)
