// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

var log = logging.GetLogger()

type Driver interface {
	fmt.Stringer
	Name() string
	Version() string
	Connect(ctx context.Context, config *types.Any) (Conn, error)
}

func New[C proto.Message](name, version string, connector Connector[C]) Driver {
	return &configurableDriver[C]{
		name:      name,
		version:   version,
		connector: connector,
	}
}

type configurableDriver[C proto.Message] struct {
	name      string
	version   string
	connector Connector[C]
}

func (d *configurableDriver[C]) Name() string {
	return d.name
}

func (d *configurableDriver[C]) Version() string {
	return d.version
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
	return newConfigurableConn[C](d, conn), nil
}

func (d *configurableDriver[C]) String() string {
	return fmt.Sprintf("%s/%s", d.name, d.version)
}

var _ Driver = (*configurableDriver[*types.Any])(nil)
