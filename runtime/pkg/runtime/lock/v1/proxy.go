// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/pkg/runtime/lock/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newLockServer(client *runtime.PrimitiveClient[Lock]) lockv1.LockServer {
	return &lockServer{
		client: client,
	}
}

type lockServer struct {
	client *runtime.PrimitiveClient[Lock]
}

func (s *lockServer) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Lock",
			logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Lock(ctx, request)
	if err != nil {
		log.Debugw("Lock",
			logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Lock",
		logging.Stringer("LockResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Unlock(ctx, request)
	if err != nil {
		log.Debugw("Unlock",
			logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Unlock",
		logging.Stringer("UnlockResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	log.Debugw("GetLock",
		logging.Stringer("GetLockRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.GetLock(ctx, request)
	if err != nil {
		log.Debugw("GetLock",
			logging.Stringer("GetLockRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetLock",
		logging.Stringer("GetLockResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

var _ lockv1.LockServer = (*lockServer)(nil)
