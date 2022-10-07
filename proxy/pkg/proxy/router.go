// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

const wildcard = "*"

type RouterConfig struct {
	Routes []RouteConfig `json:"routes" yaml:"routes"`
}

type StoreID struct {
	Namespace string `json:"namespace" yaml:"namespace"`
	Name      string `json:"name" yaml:"name"`
}

type RouteConfig struct {
	Store    StoreID           `json:"store" yaml:"store"`
	Selector map[string]string `json:"selector" yaml:"selector"`
	Services []ServiceConfig   `json:"services" yaml:"services"`
}

type ServiceConfig struct {
	Name   string                 `json:"name" yaml:"name"`
	Config map[string]interface{} `json:"config" yaml:"config"`
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

func (r *Router) Route(primitive runtime.PrimitiveMeta) (StoreID, map[string]interface{}, error) {
	for _, route := range r.routes {
		if config, ok := route.GetConfig(primitive); ok {
			return route.Store, config, nil
		}
	}
	return StoreID{}, nil, errors.NewForbidden("no route found for '%s'", primitive.Name)
}

func newRoute(route RouteConfig) *Route {
	return &Route{
		RouteConfig: route,
	}
}

type Route struct {
	RouteConfig
}

func (r *Route) GetConfig(primitive runtime.PrimitiveMeta) (map[string]interface{}, bool) {
	if r.Selector != nil && len(r.Selector) > 0 {
		if primitive.Tags == nil {
			return nil, false
		}
		for key, value := range r.Selector {
			if value == wildcard {
				if _, ok := primitive.Tags[key]; !ok {
					return nil, false
				}
			} else {
				if primitive.Tags[key] != value {
					return nil, false
				}
			}
		}
	}

	for _, service := range r.Services {
		if service.Name == primitive.Service {
			if service.Config != nil {
				return service.Config, true
			}
			return map[string]interface{}{}, true
		}
	}
	return map[string]interface{}{}, true
}
