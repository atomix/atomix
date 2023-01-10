// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	redismapv1 "github.com/atomix/atomix/drivers/redis/v8/driver/map/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/go-redis/redis/v8"
)

func newConn(client *redis.Client) driver.Conn {
	return &redisConn{
		client: client,
	}
}

type redisConn struct {
	client *redis.Client
}

func (c *redisConn) NewMapV1() mapv1.MapServer {
	return redismapv1.NewMap(c.client)
}

func (c *redisConn) Close(ctx context.Context) error {
	return c.client.Close()
}
