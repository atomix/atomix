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
	drivers      map[runtimev1.DriverID]driver.Driver
	conns        map[runtimev1.StoreID]driver.Conn
	connsMu      sync.RWMutex
	routes       []runtimev1.Route
	routesMu     sync.RWMutex
	primitives   sync.Map
	primitivesMu sync.RWMutex
}

func (r *Runtime) lookup(storeID runtimev1.StoreID) (driver.Conn, error) {
	r.connsMu.RLock()
	defer r.connsMu.RUnlock()
	conn, ok := r.conns[storeID]
	if !ok {
		return nil, errors.NewUnavailable("connection to store '%s' not found", storeID)
	}
	return conn, nil
}

func (r *Runtime) Program(ctx context.Context, routes ...runtimev1.Route) error {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()
	if r.routes != nil {
		return errors.NewForbidden("routes have already been programed")
	}
	r.routes = routes
	return nil
}

func (r *Runtime) Connect(ctx context.Context, storeID runtimev1.StoreID, driverID runtimev1.DriverID, config *types.Any) error {
	r.connsMu.Lock()
	defer r.connsMu.Unlock()

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
	r.connsMu.Lock()
	defer r.connsMu.Unlock()

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
	r.connsMu.Lock()
	defer r.connsMu.Unlock()

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
	r.routesMu.RLock()
	defer r.routesMu.RUnlock()
	if r.routes == nil {
		return runtimev1.StoreID{}, nil, errors.NewUnavailable("primitives are currently unavailable: waiting for route programming")
	}
	storeID, config, err := route(r.routes, meta)
	if err != nil {
		log.Warnf("Could not route primitive '%s' to store: %s", meta.Name, err.Error())
		return storeID, nil, err
	}
	log.Infof("Routed primitive '%s' to '%s'", meta.Name, storeID)
	return storeID, config, nil
}

func route(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) (runtimev1.StoreID, *types.Any, error) {
	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	// If the primitive name matches any rule, only those rules with matching names can be considered
	if matchesName(routes, meta) {
		routes = matchName(routes, meta)
	} else {
		routes = filterName(routes, meta)
	}

	// If the primitive tags match any rule, only those rules with matching tags can be considered
	if matchesTags(routes, meta) {
		routes = matchTags(routes, meta)
	} else {
		routes = filterTags(routes, meta)
	}

	// If the primitive type matches any rule, only those rules with matching types can be considered
	if matchesType(routes, meta) {
		routes = matchType(routes, meta)
	} else {
		routes = filterType(routes, meta)
	}

	var matchedRoute *runtimev1.Route
	var matchedRule runtimev1.RoutingRule
	for _, r := range routes {
		route := r
		if len(route.Rules) == 0 && matchedRoute == nil {
			matchedRoute = &route
		}

		for _, rule := range route.Rules {
			if matchedRoute == nil {
				matchedRoute = &route
				matchedRule = rule
			} else if len(rule.Tags) > len(matchedRule.Tags) {
				matchedRoute = &route
				matchedRule = rule
			}
		}
	}

	if matchedRoute != nil {
		return matchedRoute.StoreID, matchedRule.Config, nil
	}
	return runtimev1.StoreID{}, nil, errors.NewUnavailable("no route found matching the given primitive")
}

func matchesName(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) bool {
	for _, route := range routes {
		for _, rule := range route.Rules {
			for _, name := range rule.Names {
				if name == meta.Name {
					return true
				}
			}
		}
	}
	return false
}

func matchName(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	var namedRoutes []runtimev1.Route
	for _, route := range routes {
		var namedRules []runtimev1.RoutingRule
		for _, rule := range route.Rules {
			for _, name := range rule.Names {
				if name == meta.Name {
					namedRules = append(namedRules, rule)
					break
				}
			}
		}
		if len(namedRules) > 0 {
			namedRoutes = append(namedRoutes, runtimev1.Route{
				StoreID: route.StoreID,
				Rules:   namedRules,
			})
		}
	}
	return namedRoutes
}

func filterName(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	var filteredRoutes []runtimev1.Route
	for _, route := range routes {
		if len(route.Rules) == 0 {
			filteredRoutes = append(filteredRoutes, route)
		} else {
			var filteredRules []runtimev1.RoutingRule
			for _, rule := range route.Rules {
				if len(rule.Names) == 0 {
					filteredRules = append(filteredRules, rule)
				} else {
					hasNames := false
					for _, name := range rule.Names {
						if name != wildcard {
							hasNames = true
						}
					}
					if !hasNames {
						filteredRules = append(filteredRules, rule)
					}
				}
			}
			if len(filteredRules) > 0 {
				filteredRoutes = append(filteredRoutes, runtimev1.Route{
					StoreID: route.StoreID,
					Rules:   filteredRules,
				})
			}
		}
	}
	return filteredRoutes
}

func matchesTags(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) bool {
	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	for _, route := range routes {
		for _, rule := range route.Rules {
			if len(rule.Tags) == 0 {
				continue
			}
			allMatch := true
			for _, tag := range rule.Tags {
				if _, ok := tags[tag]; !ok {
					allMatch = false
				}
			}
			if allMatch {
				return true
			}
		}
	}
	return false
}

func matchTags(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	var taggedRoutes []runtimev1.Route
	for _, route := range routes {
		var taggedRules []runtimev1.RoutingRule
		for _, rule := range route.Rules {
			if len(rule.Tags) == 0 {
				continue
			}
			allMatch := true
			for _, tag := range rule.Tags {
				if _, ok := tags[tag]; !ok {
					allMatch = false
				}
			}
			if allMatch {
				taggedRules = append(taggedRules, rule)
			}
		}
		if len(taggedRules) > 0 {
			taggedRoutes = append(taggedRoutes, runtimev1.Route{
				StoreID: route.StoreID,
				Rules:   taggedRules,
			})
		}
	}
	return taggedRoutes
}

func filterTags(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	var filteredRoutes []runtimev1.Route
	for _, route := range routes {
		if len(route.Rules) == 0 {
			filteredRoutes = append(filteredRoutes, route)
		} else {
			var filteredRules []runtimev1.RoutingRule
			for _, rule := range route.Rules {
				if len(rule.Tags) == 0 {
					filteredRules = append(filteredRules, rule)
				}
			}
			if len(filteredRules) > 0 {
				filteredRoutes = append(filteredRoutes, runtimev1.Route{
					StoreID: route.StoreID,
					Rules:   filteredRules,
				})
			}
		}
	}
	return filteredRoutes
}

func matchesType(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) bool {
	for _, route := range routes {
		for _, rule := range route.Rules {
			if rule.Type.Name != "" && meta.Type.Name == rule.Type.Name && (rule.Type.APIVersion == "" || meta.Type.APIVersion == rule.Type.APIVersion) {
				return true
			}
		}
	}
	return false
}

func matchType(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	var typedRoutes []runtimev1.Route
	for _, route := range routes {
		var typedRules []runtimev1.RoutingRule
		for _, rule := range route.Rules {
			if rule.Type.Name != "" && meta.Type.Name == rule.Type.Name && (rule.Type.APIVersion == "" || meta.Type.APIVersion == rule.Type.APIVersion) {
				typedRules = append(typedRules, rule)
			}
		}
		if len(typedRules) > 0 {
			typedRoutes = append(typedRoutes, runtimev1.Route{
				StoreID: route.StoreID,
				Rules:   typedRules,
			})
		}
	}
	return typedRoutes
}

func filterType(routes []runtimev1.Route, meta runtimev1.PrimitiveMeta) []runtimev1.Route {
	var filteredRoutes []runtimev1.Route
	for _, route := range routes {
		if len(route.Rules) == 0 {
			filteredRoutes = append(filteredRoutes, route)
		} else {
			var filteredRules []runtimev1.RoutingRule
			for _, rule := range route.Rules {
				if rule.Type.Name == "" {
					filteredRules = append(filteredRules, rule)
				}
			}
			if len(filteredRules) > 0 {
				filteredRoutes = append(filteredRoutes, runtimev1.Route{
					StoreID: route.StoreID,
					Rules:   filteredRules,
				})
			}
		}
	}
	return filteredRoutes
}
