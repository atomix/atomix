// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"path/filepath"
	"plugin"
	"sync"
)

var log = logging.GetLogger()

func newRuntime(options Options) *Runtime {
	drivers := make(map[runtime.DriverID]runtime.Driver)
	for _, driver := range options.Drivers {
		drivers[driver.ID()] = driver
	}
	return &Runtime{
		Options: options,
		router:  newRouter(options.RouterConfig),
		drivers: drivers,
		conns:   make(map[StoreID]runtime.Conn),
	}
}

type Runtime struct {
	Options
	router  *Router
	drivers map[runtime.DriverID]runtime.Driver
	conns   map[StoreID]runtime.Conn
	mu      sync.RWMutex
}

func (r *Runtime) GetConn(primitive runtime.PrimitiveMeta) (runtime.Conn, []byte, error) {
	storeID, config, err := r.router.Route(primitive)
	if err != nil {
		return nil, nil, err
	}

	conn, ok := r.conns[storeID]
	if !ok {
		return nil, nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}

	bytes, err := json.Marshal(config)
	if err != nil {
		return nil, nil, err
	}
	return conn, bytes, nil
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
		log.Infow("Loading driver",
			logging.String("Driver", driverID.Name),
			logging.String("Version", driverID.Version))
		path := filepath.Join(r.PluginsDir, fmt.Sprintf("%s@%s.so", driverID.Name, driverID.Version))
		driverPlugin, err := plugin.Open(path)
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.Name),
				logging.String("Version", driverID.Version),
				logging.Error("Error", err))
			return err
		}
		driverSym, err := driverPlugin.Lookup("Plugin")
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.Name),
				logging.String("Version", driverID.Version),
				logging.Error("Error", err))
			return err
		}
		driver = *driverSym.(*runtime.Driver)
		r.drivers[driverID] = driver
	}

	log.Infow("Establishing connection to store",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	conn, err := driver.Connect(ctx, config)
	if err != nil {
		log.Warnw("Connecting to store failed",
			logging.String("Name", storeID.Name),
			logging.String("Namespace", storeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connected to store",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
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
	log.Infow("Reconfiguring connection to store",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	if err := conn.Configure(ctx, config); err != nil {
		log.Warnw("Reconfiguring connection to store failed",
			logging.String("Name", storeID.Name),
			logging.String("Namespace", storeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Reconfigured connection to store",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	return nil
}

func (r *Runtime) disconnect(ctx context.Context, storeID StoreID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if !ok {
		return errors.NewForbidden("connection '%s' not found", storeID)
	}
	defer delete(r.conns, storeID)

	log.Infow("Disconnecting from store",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	if err := conn.Close(ctx); err != nil {
		log.Warnw("Failed disconnecting from store",
			logging.String("Name", storeID.Name),
			logging.String("Namespace", storeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connection to store closed",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	return nil
}

var _ runtime.Runtime = (*Runtime)(nil)
