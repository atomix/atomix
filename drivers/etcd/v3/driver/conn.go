// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	etcdlockv1 "github.com/atomix/atomix/drivers/etcd/v3/driver/lock/v1"
	etcdmapv1 "github.com/atomix/atomix/drivers/etcd/v3/driver/map/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func newConn(client *clientv3.Client) (driver.Conn, error) {
	session, err := concurrency.NewSession(client)
	if err != nil {
		return nil, err
	}
	return &etcdConn{
		session: session,
	}, nil
}

type etcdConn struct {
	session *concurrency.Session
}

func (c *etcdConn) NewLockV1() lockv1.LockServer {
	return etcdlockv1.NewLock(c.session)
}

func (c *etcdConn) NewMapV1() mapv1.MapServer {
	return etcdmapv1.NewMap(c.session)
}

func (c *etcdConn) Close(ctx context.Context) error {
	return c.session.Close()
}
