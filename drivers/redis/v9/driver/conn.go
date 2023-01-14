// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	redissetv1 "github.com/atomix/atomix/drivers/redis/v9/driver/set/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/go-redis/redis/v9"
)

func newConn(client *redis.Client) driver.Conn {
	return &redisConn{
		client: client,
	}
}

type redisConn struct {
	client *redis.Client
}

func (c *redisConn) NewSetV1() setv1.SetServer {
	return redissetv1.NewSet(c.client)
}

func (c *redisConn) Close(ctx context.Context) error {
	return c.client.Close()
}
