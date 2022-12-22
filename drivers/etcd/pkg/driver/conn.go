// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	etcdlockv1 "github.com/atomix/atomix/drivers/etcd/pkg/driver/lock/v1"
	etcdmapv1 "github.com/atomix/atomix/drivers/etcd/pkg/driver/map/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	lockruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	mapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func newConn(client *clientv3.Client) (runtime.Conn, error) {
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

func (c *etcdConn) NewLock(spec runtimev1.PrimitiveSpec) (lockruntimev1.Lock, error) {
	return etcdlockv1.NewLock(c.session, toPrefix(spec.PrimitiveMeta)), nil
}

func (c *etcdConn) NewMap(spec runtimev1.PrimitiveSpec) (mapruntimev1.Map, error) {
	return etcdmapv1.NewMap(c.session, toPrefix(spec.PrimitiveMeta)), nil
}

func (c *etcdConn) Close(ctx context.Context) error {
	return c.session.Close()
}

func toPrefix(meta runtimev1.PrimitiveMeta) string {
	return fmt.Sprintf("%s/", meta.Name)
}
