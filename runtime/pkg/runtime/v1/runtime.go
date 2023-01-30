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

const wildcard = "*"

func New(opts ...Option) *Runtime {
	var options Options
	options.apply(opts...)
	return &Runtime{
		Options: options,
		drivers: make(map[runtimev1.DriverID]driver.Driver),
		conns:   make(map[runtimev1.StoreID]driver.Conn),
	}
}

type Runtime struct {
	Options
	drivers    map[runtimev1.DriverID]driver.Driver
	conns      map[runtimev1.StoreID]driver.Conn
	routes     map[runtimev1.StoreID]runtimev1.Route
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

func (r *Runtime) Program(ctx context.Context, routes ...runtimev1.Route) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routes != nil {
		return errors.NewForbidden("routes have already been programed")
	}
	r.routes = make(map[runtimev1.StoreID]runtimev1.Route)
	for _, route := range routes {
		r.routes[route.StoreID] = route
	}
	return nil
}

func (r *Runtime) Connect(ctx context.Context, storeID runtimev1.StoreID, driverID runtimev1.DriverID, config *types.Any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.conns[storeID]; ok {
		return errors.NewAlreadyExists("connection '%s' already exists", storeID)
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
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	conn, err := connect(ctx, drvr, config)
	if err != nil {
		log.Warnw("Connecting to route failed",
			logging.String("Name", storeID.Name),
			logging.String("Namespace", storeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Connected to route",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	r.conns[storeID] = conn
	return nil
}

func (r *Runtime) Configure(ctx context.Context, storeID runtimev1.StoreID, config *types.Any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[storeID]
	if !ok {
		return errors.NewNotFound("connection to '%s' not found", storeID)
	}

	log.Infow("Reconfiguring connection to route",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
	if err := configure(ctx, conn, config); err != nil {
		log.Warnw("Reconfiguring connection to route failed",
			logging.String("Name", storeID.Name),
			logging.String("Namespace", storeID.Namespace),
			logging.Error("Error", err))
		return err
	}
	log.Infow("Reconfigured connection to route",
		logging.String("Name", storeID.Name),
		logging.String("Namespace", storeID.Namespace))
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

func (r *Runtime) route(meta runtimev1.PrimitiveMeta) (runtimev1.StoreID, *types.Any, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.routes == nil {
		return runtimev1.StoreID{}, nil, errors.NewUnavailable("primitives are currently unavailable: waiting for route programming")
	}

	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	var matchRoute *runtimev1.Route
	var matchRule *runtimev1.RoutingRule
	for _, route := range r.routes {
		for _, rule := range route.Rules {
			// The primitive type must match the rule type, if specified.
			if rule.Type.Name != "" && meta.Type.Name != rule.Type.Name {
				continue
			}
			if rule.Type.APIVersion != "" && meta.Type.APIVersion != rule.Type.APIVersion {
				continue
			}

			// If any names are specified by the rule, the primitive must match one of the names (excluding wildcards).
			if len(rule.Names) > 0 {
				hasWildcard := false
				for _, name := range rule.Names {
					if name == meta.Name {
						return route.StoreID, rule.Config, nil
					}
					if name == wildcard {
						hasWildcard = true
					}
				}
				if !hasWildcard {
					continue
				}
			}

			// Determine whether all the tags required by the rule are present in the primitive metadata.
			tagsMatch := true
			for _, tag := range rule.Tags {
				if _, ok := tags[tag]; !ok {
					tagsMatch = false
					break
				}
			}

			// If all the tags matched and the rule specified more tags than the current match, update the match.
			if tagsMatch && (matchRule == nil || len(rule.Tags) > len(matchRule.Tags)) {
				matchRoute = &route
				matchRule = &rule
			}
		}
	}

	if matchRule != nil {
		return matchRoute.StoreID, matchRule.Config, nil
	}
	return runtimev1.StoreID{}, nil, errors.NewUnavailable("no route found matching the given primitive")
}
