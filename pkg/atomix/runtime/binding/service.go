// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package binding

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/controller"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/pkg/atomix/store"
)

var log = logging.GetLogger()

type Provider interface {
	Bindings() Manager
}

type Manager interface {
	store.Provider[*runtimev1.BindingId, *runtimev1.Binding]
}

type Service interface {
	Manager
	service.Service
}

func NewService(store *store.Store[*runtimev1.BindingId, *runtimev1.Binding]) Service {
	return &bindingService{
		store: store,
	}
}

type bindingService struct {
	store      *store.Store[*runtimev1.BindingId, *runtimev1.Binding]
	controller *controller.Controller[*runtimev1.BindingId]
}

func (s *bindingService) Store() *store.Store[*runtimev1.BindingId, *runtimev1.Binding] {
	return s.store
}

func (s *bindingService) Start() error {
	s.controller = newController(s.store)
	return s.controller.Start()
}

func (s *bindingService) Stop() error {
	return s.controller.Stop()
}

var _ Service = (*bindingService)(nil)
