// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type LockProvider interface {
	NewLockV1(ctx context.Context, id runtimev1.PrimitiveID) (LockProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (LockProxy, bool, error) {
	if provider, ok := conn.(LockProvider); ok {
		lock, err := provider.NewLockV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return lock, true, nil
	}
	return nil, false, nil
}

func NewLocksServer(rt *runtime.Runtime) lockv1.LocksServer {
	return &locksServer{
		manager: runtime.NewPrimitiveManager[LockProxy, *lockv1.Config](lockv1.PrimitiveType, resolve, rt),
	}
}

type locksServer struct {
	manager runtime.PrimitiveManager[*lockv1.Config]
}

func (s *locksServer) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *locksServer) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ lockv1.LocksServer = (*locksServer)(nil)
