// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

var log = logging.GetLogger()

type Driver interface {
	Connect(ctx context.Context, config *types.Any) (Conn, error)
}

func New[C proto.Message](connector Connector[C]) Driver {
	return &configurableDriver[C]{
		connector: connector,
	}
}

type configurableDriver[C proto.Message] struct {
	connector Connector[C]
}

func (d *configurableDriver[C]) Connect(ctx context.Context, rawConfig *types.Any) (Conn, error) {
	var config C
	if err := jsonpb.UnmarshalString(string(rawConfig.Value), config); err != nil {
		return nil, err
	}
	conn, err := d.connector(ctx, config)
	if err != nil {
		return nil, err
	}
	return newConfigurableConn[C](conn), nil
}

var _ Driver = (*configurableDriver[*types.Any])(nil)
