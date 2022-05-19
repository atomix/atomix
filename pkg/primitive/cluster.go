// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/errors"
)

func newConn[T Primitive](proxies *Registry[T], client *Client[T]) *Conn[T] {
	return &Conn[T]{
		proxies: proxies,
		client:  client,
	}
}

type Conn[T Primitive] struct {
	proxies *Registry[T]
	client  *Client[T]
}

func (n *Conn[T]) Create(ctx context.Context, name string) error {
	proxy, err := n.client.GetPrimitive(ctx, name)
	if err != nil {
		return err
	}
	n.proxies.register(name, proxy)
	return nil
}

func (n *Conn[T]) Close(ctx context.Context, name string) error {
	proxy, ok := n.proxies.unregister(name)
	if !ok {
		return errors.NewForbidden("proxy '%s' not found", name)
	}
	return proxy.Close(ctx)
}
