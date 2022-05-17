// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atom

import (
	"context"
	"github.com/atomix/runtime/pkg/errors"
)

func newCluster[T Atom](proxies *Registry[T], client *Client[T]) *Cluster[T] {
	return &Cluster[T]{
		proxies: proxies,
		client:  client,
	}
}

type Cluster[T Atom] struct {
	proxies *Registry[T]
	client  *Client[T]
}

func (n *Cluster[T]) CreateProxy(ctx context.Context, name string) error {
	proxy, err := n.client.GetAtom(ctx, name)
	if err != nil {
		return err
	}
	n.proxies.register(name, proxy)
	return nil
}

func (n *Cluster[T]) CloseProxy(ctx context.Context, name string) error {
	proxy, ok := n.proxies.unregister(name)
	if !ok {
		return errors.NewForbidden("proxy '%s' not found", name)
	}
	return proxy.Close(ctx)
}
