// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

type Driver interface {
	fmt.Stringer
	Kind() runtime.Kind
	Connect(ctx context.Context, config []byte) (Conn, error)
}

func New[C any](name, version string, connector Connector[C]) Driver {
	return &configurableDriver[C]{
		kind:      runtime.NewKind(name, version),
		connector: connector,
	}
}

type configurableDriver[C any] struct {
	kind      runtime.Kind
	connector Connector[C]
}

func (d *configurableDriver[C]) Kind() runtime.Kind {
	return d.kind
}

func (d *configurableDriver[C]) Connect(ctx context.Context, data []byte) (Conn, error) {
	var config C
	if data != nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}
	conn, err := d.connector(ctx, config)
	if err != nil {
		return nil, err
	}
	return newConfigurableConn[C](d, conn), nil
}

func (d *configurableDriver[C]) String() string {
	return d.kind.String()
}

var _ Driver = (*configurableDriver[any])(nil)
