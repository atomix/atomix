// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	runtime "github.com/atomix/atomix/proxy/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
)

var log = logging.GetLogger()

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func NewLockServer(rt *runtime.Runtime) lockv1.LockServer {
	return &lockServer{
		manager: runtime.NewPrimitiveManager[lockv1.LockServer](PrimitiveType, rt),
	}
}

type lockServer struct {
	manager *runtime.PrimitiveManager[lockv1.LockServer]
}

func (s *lockServer) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *lockServer) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

func (s *lockServer) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc64("LockRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		log.Warnw("Lock",
			logging.Trunc64("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Lock(ctx, request)
	if err != nil {
		log.Debugw("Lock",
			logging.Trunc64("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Lock",
		logging.Trunc64("LockResponse", response))
	return response, nil
}

func (s *lockServer) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc64("UnlockRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		log.Warnw("Unlock",
			logging.Trunc64("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Unlock(ctx, request)
	if err != nil {
		log.Debugw("Unlock",
			logging.Trunc64("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Unlock",
		logging.Trunc64("UnlockResponse", response))
	return response, nil
}

func (s *lockServer) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	log.Debugw("GetLock",
		logging.Trunc64("GetLockRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		log.Warnw("GetLock",
			logging.Trunc64("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.GetLock(ctx, request)
	if err != nil {
		log.Debugw("GetLock",
			logging.Trunc64("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetLock",
		logging.Trunc64("GetLockResponse", response))
	return response, nil
}

var _ lockv1.LockServer = (*lockServer)(nil)
