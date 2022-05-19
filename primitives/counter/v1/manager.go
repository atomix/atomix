// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newCounterV1ManagerServer(proxies *primitive.Service[Counter]) v1.CounterManagerServer {
	return &counterV1ManagerServer{
		proxies: proxies,
	}
}

type counterV1ManagerServer struct {
	proxies *primitive.Service[Counter]
}

func (s *counterV1ManagerServer) Create(ctx context.Context, request *v1.CreateRequest) (*v1.CreateResponse, error) {
	conn, err := s.proxies.Connect(ctx, request.Primitive)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	err = conn.CreateProxy(ctx, request.Name)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return &v1.CreateResponse{}, nil
}

func (s *counterV1ManagerServer) Close(ctx context.Context, request *v1.CloseRequest) (*v1.CloseResponse, error) {
	conn, err := s.proxies.Connect(ctx, request.PrimitiveID)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	err = conn.CloseProxy(ctx, request.PrimitiveID)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return &v1.CloseResponse{}, nil
}

var _ v1.CounterManagerServer = (*counterV1ManagerServer)(nil)
