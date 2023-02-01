// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoute(t *testing.T) {
	route1 := runtimev1.Route{
		StoreID: runtimev1.StoreID{
			Name: "store1",
		},
	}
	route2 := runtimev1.Route{
		StoreID: runtimev1.StoreID{
			Name: "store2",
		},
		Rules: []runtimev1.RoutingRule{
			{
				Tags: []string{"tag1"},
			},
		},
	}
	route3 := runtimev1.Route{
		StoreID: runtimev1.StoreID{
			Name: "store3",
		},
		Rules: []runtimev1.RoutingRule{
			{
				Type: runtimev1.PrimitiveType{
					Name:       "Set",
					APIVersion: "v1",
				},
				Config: &types.Any{
					Value: []byte("foo"),
				},
			},
		},
	}
	route4 := runtimev1.Route{
		StoreID: runtimev1.StoreID{
			Name: "store4",
		},
		Rules: []runtimev1.RoutingRule{
			{
				Names: []string{"primitive1"},
			},
		},
	}

	primitive1 := runtimev1.PrimitiveMeta{
		Type: runtimev1.PrimitiveType{
			Name:       "Map",
			APIVersion: "v1",
		},
		PrimitiveID: runtimev1.PrimitiveID{
			Name: "primitive1",
		},
	}
	primitive2 := runtimev1.PrimitiveMeta{
		Type: runtimev1.PrimitiveType{
			Name:       "Set",
			APIVersion: "v1",
		},
		PrimitiveID: runtimev1.PrimitiveID{
			Name: "primitive2",
		},
		Tags: []string{"tag1"},
	}

	store, config, err := route([]runtimev1.Route{route1, route2}, primitive1)
	assert.Equal(t, "store1", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route1, route2}, primitive2)
	assert.Equal(t, "store2", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route2, route1}, primitive1)
	assert.Equal(t, "store1", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route2, route1}, primitive2)
	assert.Equal(t, "store2", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route1, route3}, primitive1)
	assert.Equal(t, "store1", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route1, route3}, primitive2)
	assert.Equal(t, "store3", store.Name)
	assert.NotNil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route1, route4}, primitive1)
	assert.Equal(t, "store4", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

	store, config, err = route([]runtimev1.Route{route1, route4}, primitive2)
	assert.Equal(t, "store1", store.Name)
	assert.Nil(t, config)
	assert.NoError(t, err)

}

func TestConnect(t *testing.T) {
	conn, err := connect(context.TODO(), protoValueDriver{}, &types.Any{})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	var marshaler jsonpb.Marshaler
	chars, err := marshaler.MarshalToString(&runtimev1.PrimitiveID{})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), protoValueDriver{}, &types.Any{
		Value: []byte(chars),
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	chars, err = marshaler.MarshalToString(&runtimev1.PrimitiveID{})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), &protoPointerDriver{}, &types.Any{
		Value: []byte(chars),
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	bytes, err := json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), jsonValueDriver{}, &types.Any{
		Value: bytes,
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	bytes, err = json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), &jsonPointerDriver{}, &types.Any{
		Value: bytes,
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestConfigure(t *testing.T) {
	var marshaler jsonpb.Marshaler
	chars, err := marshaler.MarshalToString(&runtimev1.PrimitiveID{})
	assert.NoError(t, err)
	err = configure(context.TODO(), protoValueConn{}, &types.Any{
		Value: []byte(chars),
	})
	assert.NoError(t, err)

	chars, err = marshaler.MarshalToString(&runtimev1.PrimitiveID{})
	assert.NoError(t, err)
	err = configure(context.TODO(), &protoPointerConn{}, &types.Any{
		Value: []byte(chars),
	})
	assert.NoError(t, err)

	bytes, err := json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	err = configure(context.TODO(), jsonValueConn{}, &types.Any{
		Value: bytes,
	})
	assert.NoError(t, err)

	bytes, err = json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	err = configure(context.TODO(), &jsonPointerConn{}, &types.Any{
		Value: bytes,
	})
	assert.NoError(t, err)
}

type emptyDriver struct{}

func (d emptyDriver) ID() runtimev1.DriverID {
	return runtimev1.DriverID{
		Name:       "test",
		APIVersion: "test",
	}
}

func (d emptyDriver) String() string {
	return fmt.Sprintf("%s/%s", d.ID().Name, d.ID().APIVersion)
}

var _ driver.Driver = emptyDriver{}

type protoValueDriver struct {
	emptyDriver
}

func (d protoValueDriver) Connect(ctx context.Context, config *runtimev1.PrimitiveID) (driver.Conn, error) {
	return emptyConn{}, nil
}

type protoPointerDriver struct {
	emptyDriver
}

func (d *protoPointerDriver) Connect(ctx context.Context, config *runtimev1.PrimitiveID) (driver.Conn, error) {
	return emptyConn{}, nil
}

type jsonConfig struct {
	Value string `json:"value"`
}

type jsonValueDriver struct {
	emptyDriver
}

func (d jsonValueDriver) Connect(ctx context.Context, config jsonConfig) (driver.Conn, error) {
	return emptyConn{}, nil
}

type jsonPointerDriver struct {
	emptyDriver
}

func (d *jsonPointerDriver) Connect(ctx context.Context, config *jsonConfig) (driver.Conn, error) {
	return emptyConn{}, nil
}

type emptyConn struct{}

func (c emptyConn) Close(ctx context.Context) error {
	return nil
}

type protoValueConn struct {
	emptyConn
}

func (d protoValueConn) Configure(ctx context.Context, config *runtimev1.PrimitiveID) error {
	return nil
}

func (d protoValueConn) NewTestV1() runtimev1.RuntimeServer {
	return &runtimev1.UnimplementedRuntimeServer{}
}

type protoPointerConn struct {
	emptyConn
}

func (d *protoPointerConn) Configure(ctx context.Context, config *runtimev1.PrimitiveID) error {
	return nil
}

func (d *protoPointerConn) NewTestV1(config *runtimev1.PrimitiveID) (runtimev1.RuntimeServer, error) {
	return &runtimev1.UnimplementedRuntimeServer{}, nil
}

type jsonValueConn struct {
	emptyConn
}

func (d jsonValueConn) Configure(ctx context.Context, config jsonConfig) error {
	return nil
}

func (d jsonValueConn) NewTestV1() runtimev1.RuntimeServer {
	return &runtimev1.UnimplementedRuntimeServer{}
}

type jsonPointerConn struct {
	emptyConn
}

func (d *jsonPointerConn) Configure(ctx context.Context, config *jsonConfig) error {
	return nil
}

func (d *jsonPointerConn) NewTestV1(config *jsonConfig) (runtimev1.RuntimeServer, error) {
	return &runtimev1.UnimplementedRuntimeServer{}, nil
}

var _ driver.Conn = emptyConn{}
