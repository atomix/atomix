// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/store"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/version"
)

var log = logging.GetLogger()

func New(network Network, opts ...Option) *Runtime {
	var options Options
	options.apply(opts...)
	return &Runtime{
		Options:    options,
		network:    network,
		primitives: store.NewStore[*runtimev1.PrimitiveId, *runtimev1.Primitive](),
		bindings:   store.NewStore[*runtimev1.BindingId, *runtimev1.Binding](),
		clusters:   store.NewStore[*runtimev1.ClusterId, *runtimev1.Cluster](),
		drivers:    newDriverRepository(options.CacheDir, options.Drivers...),
	}
}

type Version string

type Runtime struct {
	Options
	network          Network
	primitives       *store.Store[*runtimev1.PrimitiveId, *runtimev1.Primitive]
	bindings         *store.Store[*runtimev1.BindingId, *runtimev1.Binding]
	clusters         *store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
	drivers          *driverRepository
	primitiveService service.Service
	controlService   service.Service
}

func (r *Runtime) Network() Network {
	return r.network
}

func (r *Runtime) Version() Version {
	return Version(version.Version())
}

func (r *Runtime) Start() error {
	r.controlService = newControlService(r, r.ControlService)
	if err := r.controlService.Start(); err != nil {
		return err
	}
	r.primitiveService = newProxyService(r, r.ProxyService)
	if err := r.primitiveService.Start(); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) Stop() error {
	if err := r.primitiveService.Stop(); err != nil {
		return err
	}
	if err := r.controlService.Stop(); err != nil {
		return err
	}
	return nil
}

var _ service.Service = (*Runtime)(nil)
