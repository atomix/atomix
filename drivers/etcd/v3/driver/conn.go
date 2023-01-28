// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	etcdlockv1 "github.com/atomix/atomix/drivers/etcd/v3/driver/lock/v1"
	etcdmapv1 "github.com/atomix/atomix/drivers/etcd/v3/driver/map/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	runtimelockv1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
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

func (c *etcdConn) NewLockV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimelockv1.LockProxy, error) {
	return etcdlockv1.NewLock(c.session, id)
}

func (c *etcdConn) NewMapV1(ctx context.Context, id runtimev1.PrimitiveID) (runtimemapv1.MapProxy, error) {
	return etcdmapv1.NewMap(c.session, id)
}

func (c *etcdConn) Close(ctx context.Context) error {
	return c.session.Close()
}

var _ runtimelockv1.LockProvider = (*etcdConn)(nil)
var _ runtimemapv1.MapProvider = (*etcdConn)(nil)
