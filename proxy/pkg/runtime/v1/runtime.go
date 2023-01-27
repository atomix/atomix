// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"encoding/json"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"reflect"
	"sync"
)

var log = logging.GetLogger()

func New(opts ...Option) *Runtime {
	var options Options
	options.apply(opts...)
	return &Runtime{
		Options: options,
		router:  newRouter(options.RouteProvider),
		drivers: make(map[runtimev1.DriverID]driver.Driver),
		conns:   make(map[runtimev1.StoreID]driver.Conn),
	}
}

type Runtime struct {
	Options
	router     *router
	drivers    map[runtimev1.DriverID]driver.Driver
	conns      map[runtimev1.StoreID]driver.Conn
	primitives sync.Map
	mu         sync.RWMutex
}

func (r *Runtime) route(ctx context.Context, meta runtimev1.PrimitiveMeta) (runtimev1.StoreID, *types.Any, error) {
	return r.router.route(ctx, meta)
}

func (r *Runtime) lookup(storeID runtimev1.StoreID) (driver.Conn, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.conns[storeID]
	if !ok {
		return nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}
	return conn, nil
}

func (r *Runtime) Connect(ctx context.Context, driverID runtimev1.DriverID, store runtimev1.Store) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.conns[store.StoreID]; ok {
		return errors.NewAlreadyExists("connection '%s' already exists", store.StoreID)
	}

	drvr, ok := r.drivers[driverID]
	if !ok {
		log.Infow("Loading driver",
			logging.String("Driver", driverID.Name),
			logging.String("APIVersion", driverID.APIVersion))
		var err error
		drvr, err = r.DriverProvider.LoadDriver(ctx, driverID)
		if err != nil {
			err = errors.NewInternal("failed loading driver '%s': %v", driverID, err)
			log.Warnw("Loading driver failed",
				logging.String("Driver", driverID.Name),
				logging.String("APIVersion", driverID.APIVersion),
				logging.Error("Error", err))
			return err
		}
		r.drivers[driverID] = drvr
	}

	log.Infow("Establishing connection to store",
		logging.String("Name", store.StoreID.Name),
		logging.String("Namespace", store.StoreID.Namespace))
	conn, err := connect(ctx, drvr, store.Spec)
	if err != nil {
		log.Warnw("Connecting to store failed",
			logging.String("Name", store.StoreID.Name),
			logging.String("Namespace", store.StoreID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connected to store",
		logging.String("Name", store.StoreID.Name),
		logging.String("Namespace", store.StoreID.Namespace))
	r.conns[store.StoreID] = conn
	return nil
}

func (r *Runtime) Configure(ctx context.Context, store runtimev1.Store) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[store.StoreID]
	if !ok {
		return errors.NewNotFound("connection to '%s' not found", store.StoreID)
	}

	log.Infow("Reconfiguring connection to store",
		logging.String("Name", store.StoreID.Name),
		logging.String("Namespace", store.StoreID.Namespace))
	if err := configure(ctx, conn, store.Spec); err != nil {
		log.Warnw("Reconfiguring connection to store failed",
			logging.String("Name", store.StoreID.Name),
			logging.String("Namespace", store.StoreID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Reconfigured connection to store",
		logging.String("Name", store.StoreID.Name),
		logging.String("Namespace", store.StoreID.Namespace))
	return nil
}

func (r *Runtime) Disconnect(ctx context.Context, storeID runtimev1.StoreID) error {
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

func connect(ctx context.Context, typedDriver any, rawSpec *types.Any) (driver.Conn, error) {
	value := reflect.ValueOf(typedDriver)
	if _, ok := value.Type().MethodByName("Connect"); !ok {
		return nil, errors.NewNotSupported("driver not supported")
	}

	method := value.MethodByName("Connect")
	if method.Type().NumIn() != 2 {
		panic("unexpected method signature: Connect")
	}

	param := method.Type().In(1)
	var spec any
	if param.Kind() == reflect.Pointer {
		spec = reflect.New(param.Elem()).Interface()
	} else {
		spec = reflect.New(param).Interface()
	}

	if rawSpec.Value != nil {
		if message, ok := spec.(proto.Message); ok {
			if err := jsonpb.UnmarshalString(string(rawSpec.Value), message); err != nil {
				return nil, err
			}
		} else {
			if param.Kind() == reflect.Pointer {
				if err := json.Unmarshal(rawSpec.Value, spec); err != nil {
					return nil, err
				}
			} else {
				if err := json.Unmarshal(rawSpec.Value, &spec); err != nil {
					return nil, err
				}
			}
		}
	}

	in := []reflect.Value{
		reflect.ValueOf(ctx),
	}
	if param.Kind() == reflect.Pointer {
		in = append(in, reflect.ValueOf(spec))
	} else {
		in = append(in, reflect.ValueOf(spec).Elem())
	}

	out := method.Call(in)
	if !out[1].IsNil() {
		return nil, out[1].Interface().(error)
	}
	return out[0].Interface().(driver.Conn), nil
}

func configure(ctx context.Context, typedConn any, rawSpec *types.Any) error {
	value := reflect.ValueOf(typedConn)
	if _, ok := value.Type().MethodByName("Configure"); !ok {
		return nil
	}

	method := value.MethodByName("Configure")
	if method.Type().NumIn() != 2 {
		panic("unexpected method signature: Configure")
	}

	param := method.Type().In(1)
	var spec any
	if param.Kind() == reflect.Pointer {
		spec = reflect.New(param.Elem()).Interface()
	} else {
		spec = reflect.New(param).Interface()
	}

	if message, ok := spec.(proto.Message); ok {
		if err := jsonpb.UnmarshalString(string(rawSpec.Value), message); err != nil {
			return err
		}
	} else {
		if param.Kind() == reflect.Pointer {
			if err := json.Unmarshal(rawSpec.Value, spec); err != nil {
				return err
			}
		} else {
			if err := json.Unmarshal(rawSpec.Value, &spec); err != nil {
				return err
			}
		}
	}

	in := []reflect.Value{
		reflect.ValueOf(ctx),
	}
	if param.Kind() == reflect.Pointer {
		in = append(in, reflect.ValueOf(spec))
	} else {
		in = append(in, reflect.ValueOf(spec).Elem())
	}

	out := method.Call(in)
	if !out[0].IsNil() {
		return out[0].Interface().(error)
	}
	return nil
}
