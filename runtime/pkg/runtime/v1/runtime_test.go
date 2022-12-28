// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnect(t *testing.T) {
	var marshaler jsonpb.Marshaler
	chars, err := marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	conn, err := connect(context.TODO(), protoValueDriver{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	chars, err = marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), &protoPointerDriver{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	bytes, err := json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), jsonValueDriver{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: bytes,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	bytes, err = json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	conn, err = connect(context.TODO(), &jsonPointerDriver{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: bytes,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestConfigure(t *testing.T) {
	var marshaler jsonpb.Marshaler
	chars, err := marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	err = configure(context.TODO(), protoValueConn{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)

	chars, err = marshaler.MarshalToString(&runtimev1.RuntimeConfig{})
	assert.NoError(t, err)
	err = configure(context.TODO(), &protoPointerConn{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: []byte(chars),
		},
	})
	assert.NoError(t, err)

	bytes, err := json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	err = configure(context.TODO(), jsonValueConn{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: bytes,
		},
	})
	assert.NoError(t, err)

	bytes, err = json.Marshal(jsonConfig{Value: "foo"})
	assert.NoError(t, err)
	err = configure(context.TODO(), &jsonPointerConn{}, runtimev1.Store{
		StoreID: runtimev1.StoreID{
			Namespace: "test",
			Name:      "test",
		},
		Spec: &types.Any{
			Value: bytes,
		},
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

var _ Driver = emptyDriver{}

type protoValueDriver struct {
	emptyDriver
}

func (d protoValueDriver) Connect(ctx context.Context, config *runtimev1.RuntimeConfig) (Conn, error) {
	return emptyConn{}, nil
}

type protoPointerDriver struct {
	emptyDriver
}

func (d *protoPointerDriver) Connect(ctx context.Context, config *runtimev1.RuntimeConfig) (Conn, error) {
	return emptyConn{}, nil
}

type jsonConfig struct {
	Value string `json:"value"`
}

type jsonValueDriver struct {
	emptyDriver
}

func (d jsonValueDriver) Connect(ctx context.Context, config jsonConfig) (Conn, error) {
	return emptyConn{}, nil
}

type jsonPointerDriver struct {
	emptyDriver
}

func (d *jsonPointerDriver) Connect(ctx context.Context, config *jsonConfig) (Conn, error) {
	return emptyConn{}, nil
}

type emptyConn struct{}

func (c emptyConn) Close(ctx context.Context) error {
	return nil
}

type protoValueConn struct {
	emptyConn
}

func (d protoValueConn) Configure(ctx context.Context, config *runtimev1.RuntimeConfig) error {
	return nil
}

type protoPointerConn struct {
	emptyConn
}

func (d *protoPointerConn) Configure(ctx context.Context, config *runtimev1.RuntimeConfig) error {
	return nil
}

type jsonValueConn struct {
	emptyConn
}

func (d jsonValueConn) Configure(ctx context.Context, config jsonConfig) error {
	return nil
}

type jsonPointerConn struct {
	emptyConn
}

func (d *jsonPointerConn) Configure(ctx context.Context, config *jsonConfig) error {
	return nil
}

var _ Conn = emptyConn{}
