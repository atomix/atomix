// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
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
	}
}

type PrimitiveManager[T any] struct {
	primitiveType runtimev1.PrimitiveType
	runtime       *Runtime
	primitives    sync.Map
	mu            sync.Mutex
}

func (c *PrimitiveManager[T]) Create(ctx context.Context, primitiveID runtimev1.PrimitiveID, tags []string) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var primitive T
	value, ok := c.primitives.Load(primitiveID)
	if ok {
		return value.(T), nil
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

	c.primitives.Store(primitiveID, primitive)
	return primitive, nil
}

func (c *PrimitiveManager[T]) Get(primitiveID runtimev1.PrimitiveID) (T, error) {
	var primitive T
	value, ok := c.primitives.Load(primitiveID)
	if !ok {
		return primitive, errors.NewForbidden("primitive not found for '%s'", primitiveID.Name)
	}
	return value.(T), nil
}

func (c *PrimitiveManager[T]) Close(primitiveID runtimev1.PrimitiveID) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var primitive T
	value, ok := c.primitives.LoadAndDelete(primitiveID)
	if !ok {
		return primitive, errors.NewForbidden("primitive not found for '%s'", primitiveID.Name)
	}
	return value.(T), nil
}

func create[T any](conn driver.Conn, primitive runtimev1.Primitive) (T, error) {
	var t T

	value := reflect.ValueOf(conn)

	methodName := fmt.Sprintf("New%s%s", primitive.Type.Name, strings.ToUpper(primitive.Type.APIVersion))
	if _, ok := value.Type().MethodByName(methodName); !ok {
		return t, errors.NewNotSupported("route does not support primitive type %s/%s", primitive.Type.Name, primitive.Type.APIVersion)
	}
	method := value.MethodByName(methodName)

	var err error
	var in []reflect.Value
	if method.Type().NumIn() == 0 {
		if method.Type().NumOut() != 1 {
			panic(fmt.Sprintf("unexpected signature for method %s: expected one output", methodName))
		}
		if !method.Type().Out(0).AssignableTo(reflect.TypeOf(&t).Elem()) {
			panic(fmt.Sprintf("unexpected signature for method %s: expected %s output", methodName, reflect.TypeOf(&t).Elem().Name()))
		}
	} else if method.Type().NumIn() == 1 {
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
			if primitive.Spec != nil && primitive.Spec.Value != nil {
				if message, ok := spec.(proto.Message); ok {
					if err := jsonpb.UnmarshalString(string(primitive.Spec.Value), message); err != nil {
						return t, err
					}
				} else {
					if err := json.Unmarshal(primitive.Spec.Value, spec); err != nil {
						return t, err
					}
				}
			}
			in = append(in, reflect.ValueOf(spec))
		} else {
			spec = reflect.New(specIn).Interface()
			if primitive.Spec != nil && primitive.Spec.Value != nil {
				if message, ok := spec.(proto.Message); ok {
					if err := jsonpb.UnmarshalString(string(primitive.Spec.Value), message); err != nil {
						return t, err
					}
				} else {
					if err := json.Unmarshal(primitive.Spec.Value, &spec); err != nil {
						return t, err
					}
				}
			}
			in = append(in, reflect.ValueOf(spec).Elem())
		}
	} else {
		panic(fmt.Sprintf("unexpected signature for method %s: expected 0 or 1 input(s)", methodName))
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
