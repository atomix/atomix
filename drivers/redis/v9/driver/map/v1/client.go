// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/go-redis/redis/v9"
)

func NewMap(client *redis.Client) mapv1.MapServer {
	return &redisMap{
		client: client,
	}
}

type redisMap struct {
	client *redis.Client
	prefix string
}

func key(id runtimev1.PrimitiveID, key string) string {
	return fmt.Sprintf("%s/%s", id.Name, key)
}

func (c *redisMap) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	c.prefix = fmt.Sprintf("%s/", request.ID.Name)
	return &mapv1.CreateResponse{}, nil
}

func (c *redisMap) Close(ctx context.Context, request *mapv1.CloseRequest) (*mapv1.CloseResponse, error) {
	return &mapv1.CloseResponse{}, nil
}

func (c *redisMap) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	return nil, errors.NewNotSupported("Size not supported")
}

func (c *redisMap) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	// TODO: Support prev_version for puts
	if request.PrevVersion != 0 {
		return nil, errors.NewNotSupported("prev_version not supported by Redis/v9 driver")
	}

	// TODO: Suppport TTLs
	if request.TTL != nil {
		return nil, errors.NewNotSupported("ttl not supported by Redis/v9 driver")
	}

	// TODO: Use Lua scripting to increment version
	bytes, err := c.client.GetSet(ctx, key(request.ID, request.Key), request.Value).Bytes()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}

	// TODO: Support Version for values
	return &mapv1.PutResponse{
		PrevValue: &mapv1.VersionedValue{
			Value: bytes,
		},
	}, nil
}

func (c *redisMap) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
	return nil, errors.NewNotSupported("Insert not supported")
}

func (c *redisMap) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
	return nil, errors.NewNotSupported("Update not supported")
}

func (c *redisMap) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	bytes, err := c.client.Get(ctx, key(request.ID, request.Key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, errors.NewNotFound("key '%s' not found", request.Key)
		}
		return nil, errors.NewUnknown(err.Error())
	}

	// TODO: Support Version for values
	return &mapv1.GetResponse{
		Value: mapv1.VersionedValue{
			Value: bytes,
		},
	}, nil
}

func (c *redisMap) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	// TODO: Support prev_version for removes
	if request.PrevVersion != 0 {
		return nil, errors.NewNotSupported("prev_version not supported by Redis/v9 driver")
	}

	bytes, err := c.client.GetDel(ctx, key(request.ID, request.Key)).Bytes()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}

	// TODO: Support Version for values
	return &mapv1.RemoveResponse{
		Value: mapv1.VersionedValue{
			Value: bytes,
		},
	}, nil
}

func (c *redisMap) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	return nil, errors.NewNotSupported("Clear not supported")
}

func (c *redisMap) Lock(ctx context.Context, request *mapv1.LockRequest) (*mapv1.LockResponse, error) {
	return nil, errors.NewNotSupported("Lock not supported")
}

func (c *redisMap) Unlock(ctx context.Context, request *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error) {
	return nil, errors.NewNotSupported("Unlock not supported")
}

func (c *redisMap) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	return errors.NewNotSupported("Events not supported")
}

func (c *redisMap) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	return errors.NewNotSupported("Entries not supported")
}
