// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

type PrimitiveResolver[T any] func(conn Conn) (PrimitiveProvider[T], bool)

type PrimitiveProvider[T any] func(config []byte) (T, error)

func NewPrimitiveClient[T any](primitiveType runtimev1.PrimitiveType, runtime Runtime, resolver PrimitiveResolver[T]) *PrimitiveClient[T] {
	return &PrimitiveClient[T]{
		primitiveType: primitiveType,
		runtime:       runtime,
		resolver:      resolver,
		primitives:    make(map[runtimev1.PrimitiveID]T),
	}
}

type PrimitiveClient[T any] struct {
	primitiveType runtimev1.PrimitiveType
	runtime       Runtime
	resolver      PrimitiveResolver[T]
	primitives    map[runtimev1.PrimitiveID]T
	mu            sync.RWMutex
}

func (c *PrimitiveClient[T]) Create(ctx context.Context, primitiveID runtimev1.PrimitiveID, tags []string) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	primitive, ok := c.primitives[primitiveID]
	if ok {
		return primitive, nil
	}

	route, err := c.runtime.route(ctx, tags...)
	if err != nil {
		return primitive, err
	}

	var config []byte
	for _, primitiveRoute := range route.Primitives {
		if primitiveRoute.Type == c.primitiveType {
			config = primitiveRoute.Config
			break
		}
	}

	conn, err := c.runtime.lookup(route.StoreID)
	if err != nil {
		return primitive, err
	}

	provider, ok := c.resolver(conn)
	if !ok {
		return primitive, errors.NewNotSupported("route does not support this primitive type")
	}

	primitive, err = provider(config)
	if err != nil {
		return primitive, err
	}

	c.primitives[primitiveID] = primitive
	return primitive, nil
}

func (c *PrimitiveClient[T]) Get(primitiveID runtimev1.PrimitiveID) (T, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	primitive, ok := c.primitives[primitiveID]
	if !ok {
		return primitive, errors.NewForbidden("primitive not found for '%s'", primitiveID.Name)
	}
	return primitive, nil
}

func (c *PrimitiveClient[T]) Close(primitiveID runtimev1.PrimitiveID) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.primitives[primitiveID]
	if !ok {
		return client, errors.NewForbidden("client not found for '%s'", primitiveID.Name)
	}
	delete(c.primitives, primitiveID)
	return client, nil
}
