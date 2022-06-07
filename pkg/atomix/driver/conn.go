// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

type Connector[C proto.Message] func(ctx context.Context, config C) (Client, error)

type Conn interface {
	Driver() Driver
	Client() Client
	Context() context.Context
	Configurator[*types.Any]
	Closer
}

func newConfigurableConn[C proto.Message](driver Driver, client Client) Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &configurableConn[C]{
		driver: driver,
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

type configurableConn[C proto.Message] struct {
	driver Driver
	client Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *configurableConn[C]) Driver() Driver {
	return c.driver
}

func (c *configurableConn[C]) Context() context.Context {
	return c.ctx
}

func (c *configurableConn[C]) Client() Client {
	return c.client
}

func (c *configurableConn[C]) Configure(ctx context.Context, rawConfig *types.Any) error {
	if configurator, ok := c.client.(Configurator[C]); ok {
		var config C
		if err := jsonpb.UnmarshalString(string(rawConfig.Value), config); err != nil {
			return err
		}
		return configurator.Configure(ctx, config)
	}
	return nil
}

func (c *configurableConn[C]) Close(ctx context.Context) error {
	defer c.cancel()
	return c.client.Close(ctx)
}

var _ Conn = (*configurableConn[*types.Any])(nil)
