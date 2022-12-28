// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"reflect"
	"strings"
	"sync"
)

func NewPrimitiveManager[T any](primitiveType runtimev1.PrimitiveType, runtime *Runtime) *PrimitiveManager[T] {
	return &PrimitiveManager[T]{
		primitiveType: primitiveType,
		runtime:       runtime,
		primitives:    make(map[runtimev1.PrimitiveID]T),
	}
}

type PrimitiveManager[T any] struct {
	primitiveType runtimev1.PrimitiveType
	runtime       *Runtime
	primitives    map[runtimev1.PrimitiveID]T
	mu            sync.RWMutex
}

func (c *PrimitiveManager[T]) Create(ctx context.Context, primitiveID runtimev1.PrimitiveID, tags []string) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	primitive, ok := c.primitives[primitiveID]
	if ok {
		return primitive, nil
	}

	meta := runtimev1.PrimitiveMeta{
		Type:        c.primitiveType,
		PrimitiveID: primitiveID,
		Tags:        tags,
	}

	storeID, spec, err := c.runtime.route(ctx, meta)
	if err != nil {
		return primitive, err
	}

	conn, err := c.runtime.lookup(storeID)
	if err != nil {
		return primitive, err
	}

	primitive, err = create[T](conn, runtimev1.Primitive{
		PrimitiveMeta: meta,
		Spec:          spec,
	})
	if err != nil {
		return primitive, err
	}

	c.primitives[primitiveID] = primitive
	return primitive, nil
}

func (c *PrimitiveManager[T]) Get(primitiveID runtimev1.PrimitiveID) (T, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	primitive, ok := c.primitives[primitiveID]
	if !ok {
		return primitive, errors.NewForbidden("primitive not found for '%s'", primitiveID.Name)
	}
	return primitive, nil
}

func (c *PrimitiveManager[T]) Close(primitiveID runtimev1.PrimitiveID) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.primitives[primitiveID]
	if !ok {
		return client, errors.NewForbidden("client not found for '%s'", primitiveID.Name)
	}
	delete(c.primitives, primitiveID)
	return client, nil
}

func create[T any](conn Conn, primitive runtimev1.Primitive) (T, error) {
	var t T

	methodName := fmt.Sprintf("New%s%s", primitive.Type.Name, strings.ToUpper(primitive.Type.APIVersion))

	value := reflect.ValueOf(conn)
	if _, ok := value.Type().MethodByName(methodName); !ok {
		return t, errors.NewNotSupported("route does not support primitive type %s/%s", primitive.Type.Name, primitive.Type.APIVersion)
	}

	method := value.MethodByName(methodName)
	if method.Type().NumIn() != 2 {
		panic("unexpected method signature: " + methodName)
	}

	primitiveIDType := method.Type().In(0)
	if !primitiveIDType.AssignableTo(reflect.TypeOf(runtimev1.PrimitiveID{})) {
		panic("unexpected method signature: " + methodName)
	}

	primitiveSpecType := method.Type().In(1)
	var spec any
	if primitiveSpecType.Kind() == reflect.Pointer {
		spec = reflect.New(primitiveSpecType.Elem()).Interface()
	} else {
		spec = reflect.New(primitiveSpecType).Interface()
	}

	if message, ok := spec.(proto.Message); ok {
		if err := jsonpb.UnmarshalString(string(primitive.Spec.Value), message); err != nil {
			return t, err
		}
	} else {
		if primitiveSpecType.Kind() == reflect.Pointer {
			if err := json.Unmarshal(primitive.Spec.Value, spec); err != nil {
				return t, err
			}
		} else {
			if err := json.Unmarshal(primitive.Spec.Value, &spec); err != nil {
				return t, err
			}
		}
	}

	in := []reflect.Value{
		reflect.ValueOf(primitive.PrimitiveID),
	}
	if primitiveSpecType.Kind() == reflect.Pointer {
		in = append(in, reflect.ValueOf(spec))
	} else {
		in = append(in, reflect.ValueOf(spec).Elem())
	}

	out := method.Call(in)
	if !out[1].IsNil() {
		return t, out[1].Interface().(error)
	}
	t = out[0].Interface().(T)
	return t, nil
}
