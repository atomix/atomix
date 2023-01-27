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
		drivers: make(map[runtimev1.DriverID]driver.Driver),
		conns:   make(map[runtimev1.StoreID]driver.Conn),
		routes:  make(map[runtimev1.StoreID]*runtimev1.Route),
	}
}

type Runtime struct {
	Options
	drivers    map[runtimev1.DriverID]driver.Driver
	conns      map[runtimev1.StoreID]driver.Conn
	routes     map[runtimev1.StoreID]*runtimev1.Route
	primitives sync.Map
	mu         sync.RWMutex
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

func (r *Runtime) AddRoute(ctx context.Context, route *runtimev1.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[route.StoreID]; ok {
		return errors.NewAlreadyExists("route already exists")
	}
	r.routes[route.StoreID] = route
	return nil
}

func (r *Runtime) RemoveRoute(ctx context.Context, storeID runtimev1.StoreID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[storeID]; !ok {
		return errors.NewNotFound("route not found")
	}
	delete(r.routes, storeID)
	return nil
}

func (r *Runtime) ConnectStore(ctx context.Context, driverID runtimev1.DriverID, store runtimev1.Store) error {
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

func (r *Runtime) ConfigureStore(ctx context.Context, store runtimev1.Store) error {
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

func (r *Runtime) DisconnectStore(ctx context.Context, storeID runtimev1.StoreID) error {
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

func (r *Runtime) route(ctx context.Context, meta runtimev1.PrimitiveMeta) (runtimev1.StoreID, *types.Any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	for _, route := range r.routes {
		if r.routeMatches(route, tags) {
			for _, primitive := range route.Primitives {
				if r.primitiveMatches(primitive.PrimitiveMeta, meta, tags) {
					return route.StoreID, primitive.Spec, nil
				}
			}
			return route.StoreID, nil, nil
		}
	}
	return runtimev1.StoreID{}, nil, errors.NewForbidden("no route matching tags %s", tags)
}

func (r *Runtime) routeMatches(route *runtimev1.Route, tags map[string]bool) bool {
	return tagsMatch(route.MatchTags, tags)
}

func (r *Runtime) primitiveMatches(primitive runtimev1.PrimitiveMeta, meta runtimev1.PrimitiveMeta, tags map[string]bool) bool {
	// Compare the routing rule to the primitive type name
	if primitive.Type.Name != meta.Type.Name {
		return false
	}
	// Compare the routing rule to the primitive type version
	if primitive.Type.APIVersion != meta.Type.APIVersion {
		return false
	}
	return tagsMatch(primitive.Tags, tags)
}

func tagsMatch(routeTags []string, primitiveTags map[string]bool) bool {
	// If the route requires no tags, it's always a match
	if len(routeTags) == 0 {
		return true
	}
	// If the primitive is not tagged, it won't match a route with tags
	if len(primitiveTags) == 0 {
		return false
	}
	// All tags required by the route must be present in the primitive
	for _, tag := range routeTags {
		if _, ok := primitiveTags[tag]; !ok {
			return false
		}
	}
	return true
}
