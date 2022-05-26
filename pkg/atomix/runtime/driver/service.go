// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/controller"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/plugin"
	"github.com/atomix/runtime/pkg/atomix/registry"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/pkg/atomix/store"
)

var log = logging.GetLogger()

type Provider interface {
	Drivers() Manager
}

type Manager interface {
	store.Provider[*runtimev1.DriverId, *runtimev1.Driver]
	plugin.Service[driver.Driver]
	registry.Provider[*runtimev1.DriverId, driver.Driver]
}

type Service interface {
	Manager
	service.Service
}

func NewService(store *store.Store[*runtimev1.DriverId, *runtimev1.Driver]) Service {
	return &driverService{
		store: store,
	}
}

type driverService struct {
	store      *store.Store[*runtimev1.DriverId, *runtimev1.Driver]
	controller *controller.Controller[*runtimev1.DriverId]
}

func (s *driverService) Store() *store.Store[*runtimev1.DriverId, *runtimev1.Driver] {
	return s.store
}

func (s *driverService) Start() error {
	s.controller = newController(s.store)
	return s.controller.Start()
}

func (s *driverService) Stop() error {
	return s.controller.Stop()
}

var _ Service = (*driverService)(nil)
