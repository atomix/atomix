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
	Client() Client
	Context() context.Context
	Configurator[*types.Any]
	Closer
}

func newConfigurableConn[C proto.Message](client Client) Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &configurableClient[C]{
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

type configurableClient[C proto.Message] struct {
	client Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *configurableClient[C]) Context() context.Context {
	return c.ctx
}

func (c *configurableClient[C]) Client() Client {
	return c.client
}

func (c *configurableClient[C]) Configure(ctx context.Context, rawConfig *types.Any) error {
	if configurator, ok := c.client.(Configurator[C]); ok {
		var config C
		if err := jsonpb.UnmarshalString(string(rawConfig.Value), config); err != nil {
			return err
		}
		return configurator.Configure(ctx, config)
	}
	return nil
}

func (c *configurableClient[C]) Close(ctx context.Context) error {
	defer c.cancel()
	return c.client.Close(ctx)
}

var _ Conn = (*configurableClient[*types.Any])(nil)
