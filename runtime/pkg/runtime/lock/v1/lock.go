// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type LockProxy interface {
	runtime.PrimitiveProxy
	// Lock attempts to acquire the lock
	Lock(context.Context, *lockv1.LockRequest) (*lockv1.LockResponse, error)
	// Unlock releases the lock
	Unlock(context.Context, *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error)
	// GetLock gets the lock state
	GetLock(context.Context, *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error)
}

func NewLockServer(rt *runtime.Runtime) lockv1.LockServer {
	return &lockServer{
		LocksServer: NewLocksServer(rt),
		primitives:  runtime.NewPrimitiveRegistry[LockProxy](lockv1.PrimitiveType, rt),
	}
}

type lockServer struct {
	lockv1.LocksServer
	primitives runtime.PrimitiveRegistry[LockProxy]
}

func (s *lockServer) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc64("LockRequest", request))
	client, err := s.primitives.Get(request.ID)
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
	client, err := s.primitives.Get(request.ID)
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
	client, err := s.primitives.Get(request.ID)
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
