// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"encoding/json"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreate(t *testing.T) {
	primitive, err := create[runtimev1.RuntimeServer](protoValueConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	primitive, err = create[runtimev1.RuntimeServer](&protoPointerConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	primitive, err = create[runtimev1.RuntimeServer](jsonValueConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	primitive, err = create[runtimev1.RuntimeServer](&jsonPointerConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	var marshaler jsonpb.Marshaler
	chars, err := marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	primitive, err = create[runtimev1.RuntimeServer](protoValueConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	chars, err = marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	primitive, err = create[runtimev1.RuntimeServer](&protoPointerConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	bytes, err := json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	primitive, err = create[runtimev1.RuntimeServer](jsonValueConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{
			Value: bytes,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)

	bytes, err = json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	primitive, err = create[runtimev1.RuntimeServer](&jsonPointerConn{}, runtimev1.Primitive{
		PrimitiveMeta: runtimev1.PrimitiveMeta{
			Type: runtimev1.PrimitiveType{
				Name:       "Test",
				APIVersion: "v1",
			},
			PrimitiveID: runtimev1.PrimitiveID{
				Name: "test",
			},
		},
		Spec: &types.Any{
			Value: bytes,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, primitive)
}
