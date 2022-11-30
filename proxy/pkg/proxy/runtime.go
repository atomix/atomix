// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/atomix/atomix/driver/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"path/filepath"
	"plugin"
	"sync"
)

var log = logging.GetLogger()

type driverID struct {
	name    string
	version string
}

func newRuntime(options Options) *Runtime {
	drivers := make(map[driverID]driver.Driver)
	for _, driver := range options.Drivers {
		id := driverID{
			name:    driver.Name(),
			version: driver.Version(),
		}
		drivers[id] = driver
	}
	return &Runtime{
		Options: options,
		router:  newRouter(options.Config.Router),
		drivers: drivers,
		conns:   make(map[driver.StoreID]driver.Conn),
	}
}

type Runtime struct {
	Options
	router  *Router
	drivers map[driverID]driver.Driver
	conns   map[driver.StoreID]driver.Conn
	mu      sync.RWMutex
}

func (r *Runtime) getConn(context routerContext) (driver.Conn, []byte, error) {
	storeID, config, err := r.router.Route(context)
	if err != nil {
		return nil, nil, err
	}

	r.mu.RLock()
	conn, ok := r.conns[storeID]
	r.mu.RUnlock()
	if !ok {
		return nil, nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}

	bytes, err := json.Marshal(config)
	if err != nil {
		return nil, nil, err
	}
	return conn, bytes, nil
}

func (r *Runtime) connect(ctx context.Context, driverID driverID, spec driver.ConnSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[spec.StoreID]
	if ok {
		return errors.NewAlreadyExists("connection '%s' already exists", spec.StoreID)
	}

	d, ok := r.drivers[driverID]
	if !ok {
		log.Infow("Loading driver",
			logging.String("Driver", driverID.name),
			logging.String("Version", driverID.version))
		path := filepath.Join(r.PluginsDir, fmt.Sprintf("%s@%s.so", driverID.name, driverID.version))
		driverPlugin, err := plugin.Open(path)
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.name),
				logging.String("Version", driverID.version),
				logging.Error("Error", err))
			return err
		}
		driverSym, err := driverPlugin.Lookup("Plugin")
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.name),
				logging.String("Version", driverID.version),
				logging.Error("Error", err))
			return err
		}
		d = *driverSym.(*driver.Driver)
		r.drivers[driverID] = d
	}

	log.Infow("Establishing connection to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	conn, err := d.Connect(ctx, spec)
	if err != nil {
		log.Warnw("Connecting to store failed",
			logging.String("Name", spec.StoreID.Name),
			logging.String("Namespace", spec.StoreID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connected to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	r.conns[spec.StoreID] = conn
	return nil
}

func (r *Runtime) configure(ctx context.Context, spec driver.ConnSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[spec.StoreID]
	if !ok {
		return errors.NewNotFound("connection '%s' not found", spec.StoreID)
	}

	log.Infow("Reconfiguring connection to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	if configurator, ok := conn.(driver.Configurator); ok {
		if err := configurator.Configure(ctx, spec); err != nil {
			log.Warnw("Reconfiguring connection to store failed",
				logging.String("Name", spec.StoreID.Name),
				logging.String("Namespace", spec.StoreID.Namespace),
				logging.Error("Error", err))
			return err
		}
	}
	log.Infow("Reconfigured connection to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	return nil
}

func (r *Runtime) disconnect(ctx context.Context, storeID driver.StoreID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if !ok {
		return errors.NewNotFound("connection '%s' not found", storeID)
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
