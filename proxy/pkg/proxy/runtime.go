// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"sync"
)

var log = logging.GetLogger()

func newRuntime(config RouterConfig, drivers ...runtime.Driver) *Runtime {
	driverIDs := make(map[runtime.DriverID]runtime.Driver)
	for _, driver := range drivers {
		driverIDs[driver.ID()] = driver
	}
	return &Runtime{
		router:  newRouter(config),
		drivers: driverIDs,
		conns:   make(map[StoreID]runtime.Conn),
	}
}

type Runtime struct {
	router  *Router
	drivers map[runtime.DriverID]runtime.Driver
	conns   map[StoreID]runtime.Conn
	mu      sync.RWMutex
}

func (r *Runtime) GetConn(primitive runtime.PrimitiveMeta) (runtime.Conn, error) {
	storeID, err := r.router.Route(primitive)
	if err != nil {
		return nil, err
	}

	conn, ok := r.conns[storeID]
	if !ok {
		return nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}
	return conn, nil
}

func (r *Runtime) connect(ctx context.Context, storeID StoreID, driverID runtime.DriverID, config []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if ok {
		return nil
	}

	driver, ok := r.drivers[driverID]
	if !ok {
		return errors.NewNotSupported("driver '%s' not found", driverID)
	}

	conn, err := driver.Connect(ctx, config)
	if err != nil {
		return err
	}
	r.conns[storeID] = conn
	return nil
}

func (r *Runtime) configure(ctx context.Context, storeID StoreID, config []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if !ok {
		return errors.NewForbidden("connection '%s' not found", storeID)
	}
	return conn.Configure(ctx, config)
}

func (r *Runtime) disconnect(ctx context.Context, storeID StoreID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if !ok {
		return errors.NewForbidden("connection '%s' not found", storeID)
	}
	defer delete(r.conns, storeID)
	return conn.Close(ctx)
}

var _ runtime.Runtime = (*Runtime)(nil)
