// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

const wildcard = "*"

type routerContext struct {
	service string
	name    string
	tags    []string
}

func newRouter(config RouterConfig) *Router {
	routes := make([]*Route, len(config.Routes))
	for i, binding := range config.Routes {
		routes[i] = newRoute(binding)
	}
	return &Router{
		routes: routes,
	}
}

type Router struct {
	routes []*Route
}

func (r *Router) Route(context routerContext) (runtime.StoreID, map[string]interface{}, error) {
	for _, route := range r.routes {
		if config, ok := route.GetConfig(context); ok {
			return runtime.StoreID{
				Namespace: route.Store.Namespace,
				Name:      route.Store.Name,
			}, config, nil
		}
	}
	return runtime.StoreID{}, nil, errors.NewForbidden("no route found for '%s'", context.name)
}

func newRoute(route RouteConfig) *Route {
	return &Route{
		RouteConfig: route,
	}
}

type Route struct {
	RouteConfig
}

func (r *Route) GetConfig(context routerContext) (map[string]interface{}, bool) {
	if len(r.Tags) > 0 {
		if context.tags == nil {
			return nil, false
		}
		for _, tag := range r.Tags {
			for _, value := range context.tags {
				if tag != value {
					return nil, false
				}
			}
		}
	}

	for _, service := range r.Services {
		if service.Name == context.service {
			if service.Config != nil {
				return service.Config, true
			}
			return map[string]interface{}{}, true
		}
	}
	return map[string]interface{}{}, true
}
