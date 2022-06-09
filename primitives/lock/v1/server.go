// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
)

func newLockServer(proxies *primitive.Manager[lockv1.LockClient]) lockv1.LockServer {
	return &lockServer{
		proxies: proxies,
	}
}

type lockServer struct {
	proxies *primitive.Manager[lockv1.LockClient]
}

func (s *lockServer) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	proxy, err := s.proxies.Connect(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *lockServer) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	proxy, err := s.proxies.Close(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *lockServer) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Stringer("LockRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Lock(ctx, request)
	if err != nil {
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Lock",
		logging.Stringer("LockResponse", response))
	return response, nil
}

func (s *lockServer) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Unlock(ctx, request)
	if err != nil {
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Unlock",
		logging.Stringer("UnlockResponse", response))
	return response, nil
}

func (s *lockServer) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	log.Debugw("GetLock",
		logging.Stringer("GetLockRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.GetLock(ctx, request)
	if err != nil {
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetLock",
		logging.Stringer("GetLockResponse", response))
	return response, nil
}

var _ lockv1.LockServer = (*lockServer)(nil)
