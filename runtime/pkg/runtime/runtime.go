// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/network"
	"google.golang.org/grpc"
	"os"
	"sync"
)

var log = logging.GetLogger()

const (
	namespaceEnv = "ATOMIX_NAMESPACE"
)

func Namespace() string {
	return os.Getenv(namespaceEnv)
}

type Runtime interface {
	network.Service
	connect(ctx context.Context, driverID runtimev1.DriverID, spec runtimev1.ConnSpec) error
	configure(ctx context.Context, spec runtimev1.ConnSpec) error
	disconnect(ctx context.Context, storeID runtimev1.StoreID) error
	route(ctx context.Context, tags ...string) (*runtimev1.Route, error)
	lookup(storeID runtimev1.StoreID) (Conn, error)
}

func New(opts ...Option) Runtime {
	var options Options
	options.apply(opts...)
	rt := &runtime{
		Options: options,
		router:  newRouter(options.RouteProvider),
		drivers: make(map[runtimev1.DriverID]Driver),
		conns:   make(map[runtimev1.StoreID]Conn),
	}

	server := grpc.NewServer(options.GRPCServerOptions...)
	runtimev1.RegisterRuntimeServer(server, newRuntimeServer(rt))

	rt.Service = network.NewService(server,
		network.WithDriver(network.NewDefaultDriver()),
		network.WithHost(options.Host),
		network.WithPort(options.Port))
	return rt
}

type runtime struct {
	network.Service
	Options
	router  *router
	drivers map[runtimev1.DriverID]Driver
	conns   map[runtimev1.StoreID]Conn
	mu      sync.RWMutex
}

func (r *runtime) route(ctx context.Context, tags ...string) (*runtimev1.Route, error) {
	return r.router.route(ctx, tags...)
}

func (r *runtime) lookup(storeID runtimev1.StoreID) (Conn, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.conns[storeID]
	if !ok {
		return nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}
	return conn, nil
}

func (r *runtime) connect(ctx context.Context, driverID runtimev1.DriverID, spec runtimev1.ConnSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[spec.StoreID]
	if ok {
		return errors.NewAlreadyExists("connection '%s' already exists", spec.StoreID)
	}

	driver, ok := r.drivers[driverID]
	if !ok {
		log.Infow("Loading driver",
			logging.String("Driver", driverID.Name),
			logging.String("Version", driverID.Version))
		var err error
		driver, err = r.DriverProvider.LoadDriver(ctx, driverID)
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.Name),
				logging.String("Version", driverID.Version),
				logging.Error("Error", err))
			return err
		}
		r.drivers[driverID] = driver
	}

	log.Infow("Establishing connection to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	conn, err := driver.Connect(ctx, spec)
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

func (r *runtime) configure(ctx context.Context, spec runtimev1.ConnSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[spec.StoreID]
	if !ok {
		return errors.NewNotFound("connection to '%s' not found", spec.StoreID)
	}

	log.Infow("Reconfiguring connection to store",
		logging.String("Name", spec.StoreID.Name),
		logging.String("Namespace", spec.StoreID.Namespace))
	if configurator, ok := conn.(Configurator); ok {
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

func (r *runtime) disconnect(ctx context.Context, storeID runtimev1.StoreID) error {
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
