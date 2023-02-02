// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type SetProvider interface {
	NewSetV1(ctx context.Context, id runtimev1.PrimitiveID) (SetProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (SetProxy, bool, error) {
	if provider, ok := conn.(SetProvider); ok {
		set, err := provider.NewSetV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return set, true, nil
	}
	return nil, false, nil
}

func NewSetsServer(rt *runtime.Runtime) setv1.SetsServer {
	return &setsServer{
		manager: runtime.NewPrimitiveManager[SetProxy, *setv1.Config](setv1.PrimitiveType, resolve, rt),
	}
}

type setsServer struct {
	manager runtime.PrimitiveManager[*setv1.Config]
}

func (s *setsServer) Create(ctx context.Context, request *setv1.CreateRequest) (*setv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &setv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *setsServer) Close(ctx context.Context, request *setv1.CloseRequest) (*setv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &setv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ setv1.SetsServer = (*setsServer)(nil)
