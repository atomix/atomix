// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"fmt"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func NewLock(session *concurrency.Session) lockv1.LockServer {
	return &etcdLock{
		session: session,
	}
}

type etcdLock struct {
	session *concurrency.Session
	mutex   *concurrency.Mutex
}

func (s *etcdLock) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
	s.mutex = concurrency.NewMutex(s.session, fmt.Sprintf("%s/", request.ID.Name))
	return &lockv1.CreateResponse{}, nil
}

func (s *etcdLock) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
	return &lockv1.CloseResponse{}, nil
}

func (s *etcdLock) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	if err := s.mutex.Lock(ctx); err != nil {
		return nil, err
	}
	return &lockv1.LockResponse{
		Version: uint64(s.mutex.Header().Revision),
	}, nil
}

func (s *etcdLock) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	if err := s.mutex.Unlock(ctx); err != nil {
		return nil, err
	}
	return &lockv1.UnlockResponse{}, nil
}

func (s *etcdLock) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	return &lockv1.GetLockResponse{
		Version: uint64(s.mutex.Header().Revision),
	}, nil
}
