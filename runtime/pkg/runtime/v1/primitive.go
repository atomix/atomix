// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"reflect"
)

type PrimitiveProxy interface {
	Close(ctx context.Context) error
}

type Resolver[P PrimitiveProxy] func(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (P, bool, error)

type PrimitiveManager[C proto.Message] interface {
	Create(ctx context.Context, primitiveID runtimev1.PrimitiveID, tags []string) (C, error)
	Close(ctx context.Context, primitiveID runtimev1.PrimitiveID) error
}

type PrimitiveRegistry[P PrimitiveProxy] interface {
	Get(primitiveID runtimev1.PrimitiveID) (P, error)
}

func NewPrimitiveManager[P PrimitiveProxy, C proto.Message](primitiveType runtimev1.PrimitiveType, resolver Resolver[P], runtime *Runtime) PrimitiveManager[C] {
	return &primitiveManager[P, C]{
		primitiveType: primitiveType,
		resolver:      resolver,
		runtime:       runtime,
	}
}

type primitiveManager[P PrimitiveProxy, C proto.Message] struct {
	primitiveType runtimev1.PrimitiveType
	resolver      Resolver[P]
	runtime       *Runtime
}

func (c *primitiveManager[P, C]) Create(ctx context.Context, primitiveID runtimev1.PrimitiveID, tags []string) (C, error) {
	var config C

	// Route the store and spec for the primitive
	storeID, spec, err := c.runtime.route(runtimev1.PrimitiveMeta{
		Type:        c.primitiveType,
		PrimitiveID: primitiveID,
		Tags:        tags,
	})
	if err != nil {
		return config, err
	}

	// Parse the primitive configuration from the matched route spec
	configType := reflect.TypeOf(config)
	config = reflect.New(configType.Elem()).Interface().(C)
	if spec != nil && spec.Value != nil {
		if err := jsonpb.UnmarshalString(string(spec.Value), config); err != nil {
			return config, errors.NewInternal("invalid route configuration for primitive type '%s/%s'", c.primitiveType.Name, c.primitiveType.APIVersion)
		}
	}

	// Lookup the connection via the runtime
	conn, err := c.runtime.lookup(storeID)
	if err != nil {
		return config, err
	}

	c.runtime.mu.Lock()
	defer c.runtime.mu.Unlock()

	// Create and cache the primitive instance if it doesn't already exist
	value, ok := c.runtime.primitives.Load(primitiveID)
	if ok {
		if _, ok := value.(P); !ok {
			return config, errors.NewForbidden("cannot create primitive of type '%s/%s': a primitive of another type already exists with that name", c.primitiveType.Name, c.primitiveType.APIVersion)
		}
		return config, errors.NewAlreadyExists("the primitive is already open")
	}

	// Attempt to create the primitive via the driver connection
	primitive, ok, err := c.resolver(ctx, conn, primitiveID)
	if !ok {
		return config, errors.NewNotSupported("primitive type '%s/%s' not supported by configured driver", c.primitiveType.Name, c.primitiveType.APIVersion)
	}

	// Store the primitive in the cache
	c.runtime.primitives.Store(primitiveID, primitive)
	return config, nil
}

func (c *primitiveManager[P, C]) Close(ctx context.Context, primitiveID runtimev1.PrimitiveID) error {
	c.runtime.mu.Lock()
	defer c.runtime.mu.Unlock()
	value, ok := c.runtime.primitives.LoadAndDelete(primitiveID)
	if !ok {
		return errors.NewNotFound("primitive '%s' not found", primitiveID.Name)
	}
	primitive, ok := value.(P)
	if !ok {
		return errors.NewForbidden("cannot close primitive of type '%s/%s': a primitive of another type already exists with that name", c.primitiveType.Name, c.primitiveType.APIVersion)
	}
	return primitive.Close(ctx)
}

func NewPrimitiveRegistry[P PrimitiveProxy](primitiveType runtimev1.PrimitiveType, runtime *Runtime) PrimitiveRegistry[P] {
	return &primitiveRegistry[P]{
		primitiveType: primitiveType,
		runtime:       runtime,
	}
}

type primitiveRegistry[P PrimitiveProxy] struct {
	primitiveType runtimev1.PrimitiveType
	runtime       *Runtime
}

func (c *primitiveRegistry[P]) Get(primitiveID runtimev1.PrimitiveID) (P, error) {
	var primitive P
	value, ok := c.runtime.primitives.Load(primitiveID)
	if !ok {
		return primitive, errors.NewForbidden("primitive not found for '%s'", primitiveID.Name)
	}
	return value.(P), nil
}
