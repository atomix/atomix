// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newLockServer(proxies *primitive.ProxyManager[Lock]) v1.LockServer {
	return &lockServer{
		proxies: proxies,
	}
}

type lockServer struct {
	proxies *primitive.ProxyManager[Lock]
}

func (s *lockServer) Lock(ctx context.Context, request *v1.LockRequest) (*v1.LockResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Lock(ctx, request)
}

func (s *lockServer) Unlock(ctx context.Context, request *v1.UnlockRequest) (*v1.UnlockResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Unlock(ctx, request)
}

func (s *lockServer) GetLock(ctx context.Context, request *v1.GetLockRequest) (*v1.GetLockResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.GetLock(ctx, request)
}

var _ v1.LockServer = (*lockServer)(nil)
