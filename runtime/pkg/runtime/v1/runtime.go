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
		conns:   make(map[runtimev1.RouteID]driver.Conn),
		routes:  make(map[runtimev1.RouteID]runtimev1.Route),
	}
}

type Runtime struct {
	Options
	drivers    map[runtimev1.DriverID]driver.Driver
	conns      map[runtimev1.RouteID]driver.Conn
	routes     map[runtimev1.RouteID]runtimev1.Route
	primitives sync.Map
	mu         sync.RWMutex
}

func (r *Runtime) lookup(routeID runtimev1.RouteID) (driver.Conn, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.conns[routeID]
	if !ok {
		return nil, errors.NewUnavailable("connection to store '%s' not found", routeID)
	}
	return conn, nil
}

func (r *Runtime) ConnectRoute(ctx context.Context, driverID runtimev1.DriverID, route runtimev1.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.conns[route.RouteID]; ok {
		return errors.NewAlreadyExists("connection '%s' already exists", route.RouteID)
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

	log.Infow("Establishing connection to route",
		logging.String("Name", route.RouteID.Name),
		logging.String("Namespace", route.RouteID.Namespace))
	conn, err := connect(ctx, drvr, route.Config)
	if err != nil {
		log.Warnw("Connecting to route failed",
			logging.String("Name", route.RouteID.Name),
			logging.String("Namespace", route.RouteID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connected to route",
		logging.String("Name", route.RouteID.Name),
		logging.String("Namespace", route.RouteID.Namespace))
	r.conns[route.RouteID] = conn
	r.routes[route.RouteID] = route
	return nil
}

func (r *Runtime) ConfigureRoute(ctx context.Context, route runtimev1.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[route.RouteID]
	if !ok {
		return errors.NewNotFound("connection to '%s' not found", route.RouteID)
	}

	log.Infow("Reconfiguring connection to route",
		logging.String("Name", route.RouteID.Name),
		logging.String("Namespace", route.RouteID.Namespace))
	if err := configure(ctx, conn, route.Config); err != nil {
		log.Warnw("Reconfiguring connection to route failed",
			logging.String("Name", route.RouteID.Name),
			logging.String("Namespace", route.RouteID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Reconfigured connection to route",
		logging.String("Name", route.RouteID.Name),
		logging.String("Namespace", route.RouteID.Namespace))
	r.routes[route.RouteID] = route
	return nil
}

func (r *Runtime) DisconnectRoute(ctx context.Context, routeID runtimev1.RouteID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[routeID]
	if !ok {
		return errors.NewNotFound("connection '%s' not found", routeID)
	}
	defer delete(r.conns, routeID)

	log.Infow("Disconnecting from store",
		logging.String("Name", routeID.Name),
		logging.String("Namespace", routeID.Namespace))
	if err := conn.Close(ctx); err != nil {
		log.Warnw("Failed disconnecting from store",
			logging.String("Name", routeID.Name),
			logging.String("Namespace", routeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connection to store closed",
		logging.String("Name", routeID.Name),
		logging.String("Namespace", routeID.Namespace))
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

func (r *Runtime) route(meta runtimev1.PrimitiveMeta) (runtimev1.RouteID, *types.Any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	var matchRoute *runtimev1.Route
	var matchRule *runtimev1.RoutingRule
	var matchScore int
	for _, route := range r.routes {
		for _, rule := range route.Rules {
			if exactMatch(rule, meta) {
				return route.RouteID, rule.Config, true
			}
			if score, ok := scoreMatch(rule, tags); ok && score > matchScore {
				matchRoute = &route
				matchRule = &rule
				matchScore = score
			}
		}
	}

	if matchRule != nil {
		return matchRoute.RouteID, matchRule.Config, true
	}
	return runtimev1.RouteID{}, nil, false
}

func exactMatch(rule runtimev1.RoutingRule, primitive runtimev1.PrimitiveMeta) bool {
	if rule.Type.Name != "" && primitive.Type.Name != rule.Type.Name {
		return false
	}
	if rule.Type.APIVersion != "" && primitive.Type.APIVersion != rule.Type.APIVersion {
		return false
	}
	for _, id := range rule.Primitives {
		if primitive.Name == id.Name {
			return true
		}
	}
	return false
}

func scoreMatch(rule runtimev1.RoutingRule, tags map[string]bool) (int, bool) {
	for _, tag := range rule.Tags {
		if _, ok := tags[tag]; !ok {
			return 0, false
		}
	}
	return len(rule.Tags), true
}
