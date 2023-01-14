// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	"github.com/go-redis/redis/v9"
)

func NewSet(client *redis.Client) setv1.SetServer {
	return &redisSet{
		client: client,
	}
}

type redisSet struct {
	client *redis.Client
}

func (c *redisSet) Create(ctx context.Context, request *setv1.CreateRequest) (*setv1.CreateResponse, error) {
	return &setv1.CreateResponse{}, nil
}

func (c *redisSet) Close(ctx context.Context, request *setv1.CloseRequest) (*setv1.CloseResponse, error) {
	return &setv1.CloseResponse{}, nil
}

func (c *redisSet) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
	size, err := c.client.SCard(ctx, request.ID.Name).Result()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}
	return &setv1.SizeResponse{
		Size_: uint32(size),
	}, nil
}

func (c *redisSet) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	isMember, err := c.client.SIsMember(ctx, request.ID.Name, request.Element.Value).Result()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}
	return &setv1.ContainsResponse{
		Contains: isMember,
	}, nil
}

func (c *redisSet) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	count, err := c.client.SAdd(ctx, request.ID.Name, request.Element.Value).Result()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}
	return &setv1.AddResponse{
		Added: count > 0,
	}, nil
}

func (c *redisSet) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	count, err := c.client.SRem(ctx, request.ID.Name, request.Element.Value).Result()
	if err != nil {
		return nil, errors.NewUnknown(err.Error())
	}
	return &setv1.RemoveResponse{
		Removed: count > 0,
	}, nil
}

func (c *redisSet) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	var cursor uint64
	var err error
	for {
		cursor, err = c.scan(request, server, cursor, 1000)
		if err != nil {
			return err
		}
	}
}

func (c *redisSet) scan(request *setv1.ElementsRequest, server setv1.Set_ElementsServer, cursor uint64, batchSize int64) (uint64, error) {
	keys, newCursor, err := c.client.SScan(server.Context(), request.ID.Name, cursor, "*", batchSize).Result()
	if err != nil {
		return 0, errors.NewUnknown(err.Error())
	}
	for _, key := range keys {
		err := server.Send(&setv1.ElementsResponse{
			Element: setv1.Element{
				Value: key,
			},
		})
		if err != nil {
			return 0, errors.NewUnknown(err.Error())
		}
	}
	return newCursor, nil
}

func (c *redisSet) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
	return nil, errors.NewNotSupported("Clear not supported")
}

func (c *redisSet) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	return errors.NewNotSupported("Events not supported")
}
