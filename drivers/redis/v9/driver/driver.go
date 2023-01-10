// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/go-redis/redis/v9"
)

var log = logging.GetLogger()

func New() driver.Driver {
	return &redisDriver{}
}

type redisDriver struct{}

func (d *redisDriver) Connect(ctx context.Context, options *redis.Options) (driver.Conn, error) {
	client := redis.NewClient(options)
	return newConn(client), nil
}
