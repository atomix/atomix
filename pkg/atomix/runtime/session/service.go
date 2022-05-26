// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package session

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/controller"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/pkg/atomix/store"
)

var log = logging.GetLogger()

type Provider interface {
	Sessions() Manager
}

type Manager interface {
	store.Provider[*runtimev1.SessionId, *runtimev1.Session]
}

type Service interface {
	Manager
	service.Service
}

func NewService(store *store.Store[*runtimev1.SessionId, *runtimev1.Session]) Service {
	return &proxyService{
		store: store,
	}
}

type proxyService struct {
	store      *store.Store[*runtimev1.SessionId, *runtimev1.Session]
	controller *controller.Controller[*runtimev1.SessionId]
}

func (s *proxyService) Store() *store.Store[*runtimev1.SessionId, *runtimev1.Session] {
	return s.store
}

func (s *proxyService) Start() error {
	s.controller = newController(s.store)
	return s.controller.Start()
}

func (s *proxyService) Stop() error {
	return s.controller.Stop()
}

var _ Service = (*proxyService)(nil)
