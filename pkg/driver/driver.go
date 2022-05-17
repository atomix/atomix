// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/runtime/pkg/config"
	"github.com/atomix/runtime/pkg/logging"
)

var log = logging.GetLogger()

type Driver interface {
	Connect(ctx context.Context, config []byte) (Conn, error)
}

func New[C config.Config](connector Connector[C], codec config.Codec[C]) Driver {
	return &configurableDriver[C]{
		connector: connector,
		codec:     codec,
	}
}

type configurableDriver[C config.Config] struct {
	connector Connector[C]
	codec     config.Codec[C]
}

func (d *configurableDriver[C]) Connect(ctx context.Context, bytes []byte) (Conn, error) {
	config, err := d.codec.Decode(bytes)
	if err != nil {
		return nil, err
	}
	conn, err := d.connector(ctx, config)
	if err != nil {
		return nil, err
	}
	return newConfigurableConn[C](conn, d.codec), nil
}

var _ Driver = (*configurableDriver[config.Config])(nil)
