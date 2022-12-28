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

	if method.Type().NumIn() > 1 {
		panic(fmt.Sprintf("unexpected signature for method %s: expected <=1 input(s)", methodName))
	}

	var err error
	var in []reflect.Value
	if method.Type().NumIn() == 1 {
		if method.Type().NumOut() != 2 {
			panic(fmt.Sprintf("unexpected signature for method %s: expected two outputs", methodName))
		}
		if !method.Type().Out(0).AssignableTo(reflect.TypeOf(&t).Elem()) {
			panic(fmt.Sprintf("unexpected signature for method %s: expected %s output", methodName, reflect.TypeOf(&t).Elem().Name()))
		}
		if !method.Type().Out(1).AssignableTo(reflect.TypeOf(&err).Elem()) {
			panic(fmt.Sprintf("unexpected signature for method %s: expected error output", methodName))
		}

		specIn := method.Type().In(0)
		var spec any
		if specIn.Kind() == reflect.Pointer {
			spec = reflect.New(specIn.Elem()).Interface()
			if message, ok := spec.(proto.Message); ok {
				if err := jsonpb.UnmarshalString(string(primitive.Spec.Value), message); err != nil {
					return t, err
				}
			} else {
				if err := json.Unmarshal(primitive.Spec.Value, spec); err != nil {
					return t, err
				}
			}
			in = append(in, reflect.ValueOf(spec))
		} else {
			spec = reflect.New(specIn).Interface()
			if message, ok := spec.(proto.Message); ok {
				if err := jsonpb.UnmarshalString(string(primitive.Spec.Value), message); err != nil {
					return t, err
				}
			} else {
				if err := json.Unmarshal(primitive.Spec.Value, &spec); err != nil {
					return t, err
				}
			}
			in = append(in, reflect.ValueOf(spec).Elem())
		}
	} else {
		if method.Type().NumOut() != 1 {
			panic(fmt.Sprintf("unexpected signature for method %s: expected one output", methodName))
		}
		if !method.Type().Out(0).AssignableTo(reflect.TypeOf(&t).Elem()) {
			panic(fmt.Sprintf("unexpected signature for method %s: expected %s output", methodName, reflect.TypeOf(&t).Elem().Name()))
		}
	}

	out := method.Call(in)
	if len(out) == 2 {
		if !out[1].IsNil() {
			err = out[1].Interface().(error)
		} else {
			t = out[0].Interface().(T)
		}
	} else {
		t = out[0].Interface().(T)
	}
	return t, err
}
