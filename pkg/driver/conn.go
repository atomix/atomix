// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/types"
)

type Connector[C any] func(ctx context.Context, config C) (Client, error)

type ConnID struct {
	Namespace string
	Name      string
}

func (i ConnID) String() string {
	return fmt.Sprintf("%s.%s", i.Namespace, i.Name)
}

type Conn interface {
	ID() ConnID
	Driver() Driver
	Client() Client
	Context() context.Context
	Configurator[[]byte]
	Closer
}

func newConfigurableConn[C any](id ConnID, driver Driver, client Client) Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &configurableConn[C]{
		id:     id,
		driver: driver,
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

type configurableConn[C any] struct {
	id     ConnID
	driver Driver
	client Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *configurableConn[C]) ID() ConnID {
	return c.id
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

func (c *configurableConn[C]) Configure(ctx context.Context, data []byte) error {
	if configurator, ok := c.client.(Configurator[C]); ok {
		var config C
		if data != nil {
			if err := json.Unmarshal(data, &config); err != nil {
				return err
			}
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
