// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/controller"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/pkg/atomix/store"
)

var log = logging.GetLogger()

type Provider interface {
	Clusters() Manager
}

type Manager interface {
	store.Provider[*runtimev1.ClusterId, *runtimev1.Cluster]
}

type Service interface {
	Manager
	service.Service
}

func NewService(store *store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]) Service {
	return &clusterService{
		store: store,
	}
}

type clusterService struct {
	store      *store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
	controller *controller.Controller[*runtimev1.ClusterId]
}

func (s *clusterService) Store() *store.Store[*runtimev1.ClusterId, *runtimev1.Cluster] {
	return s.store
}

func (s *clusterService) Start() error {
	s.controller = newController(s.store)
	return s.controller.Start()
}

func (s *clusterService) Stop() error {
	return s.controller.Stop()
}

var _ Service = (*clusterService)(nil)
