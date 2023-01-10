// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var log = logging.GetLogger()

func New() driver.Driver {
	return &etcdDriver{}
}

type etcdDriver struct{}

func (d *etcdDriver) Connect(ctx context.Context, config clientv3.Config) (driver.Conn, error) {
	client, err := clientv3.New(config)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return newConn(client)
}
