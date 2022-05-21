// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
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

func (n *Conn[T]) Create(ctx context.Context, primitiveID runtimev1.ObjectId) error {
	proxy, err := n.client.GetPrimitive(ctx, primitiveID)
	if err != nil {
		return err
	}
	n.proxies.register(primitiveID, proxy)
	return nil
}

func (n *Conn[T]) Close(ctx context.Context, primitiveID runtimev1.ObjectId) error {
	proxy, ok := n.proxies.unregister(primitiveID)
	if !ok {
		return errors.NewForbidden("proxy '%s' not found", primitiveID.Name)
	}
	return proxy.Close(ctx)
}
