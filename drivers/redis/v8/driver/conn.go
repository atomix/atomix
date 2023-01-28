// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	redissetv1 "github.com/atomix/atomix/drivers/redis/v8/driver/set/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	runtimesetv1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
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

func (c *redisConn) NewSetV1(context.Context, runtimev1.PrimitiveID) (runtimesetv1.SetProxy, error) {
	return redissetv1.NewSet(c.client), nil
}

func (c *redisConn) Close(ctx context.Context) error {
	return c.client.Close()
}

var _ runtimesetv1.SetProvider = (*redisConn)(nil)
