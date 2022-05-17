// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/runtime/pkg/config"
)

type Connector[C any] func(ctx context.Context, config C) (Client, error)

type Conn interface {
	Client() Client
	Context() context.Context
	Configurator[[]byte]
	Closer
}

func newConfigurableConn[C any](client Client, codec config.Codec[C]) Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &configurableClient[C]{
		client: client,
		codec:  codec,
		ctx:    ctx,
		cancel: cancel,
	}
}

type configurableClient[C any] struct {
	client Client
	codec  config.Codec[C]
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *configurableClient[C]) Context() context.Context {
	return c.ctx
}

func (c *configurableClient[C]) Client() Client {
	return c.client
}

func (c *configurableClient[C]) Configure(ctx context.Context, bytes []byte) error {
	if configurator, ok := c.client.(Configurator[C]); ok {
		config, err := c.codec.Decode(bytes)
		if err != nil {
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

var _ Conn = (*configurableClient[any])(nil)
