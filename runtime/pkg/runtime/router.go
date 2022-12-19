// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
)

type RouteProvider interface {
	LoadRoutes(ctx context.Context) ([]*runtimev1.Route, error)
}

func newStaticRouteProvider(routes ...*runtimev1.Route) RouteProvider {
	return &staticRouteProvider{
		routes: routes,
	}
}

type staticRouteProvider struct {
	routes []*runtimev1.Route
}

func (p *staticRouteProvider) LoadRoutes(ctx context.Context) ([]*runtimev1.Route, error) {
	return p.routes, nil
}

func newRouter(provider RouteProvider) *router {
	return &router{
		provider: provider,
	}
}

type router struct {
	provider RouteProvider
}

func (r *router) route(ctx context.Context, tags ...string) (*runtimev1.Route, error) {
	routes, err := r.provider.LoadRoutes(ctx)
	if err != nil {
		return nil, errors.NewInternal(err.Error())
	}

	for _, route := range routes {
		if r.routeMatches(route, tags...) {
			return route, err
		}
	}
	return nil, errors.NewForbidden("no route matching tags %s", tags)
}

func (r *router) routeMatches(route *runtimev1.Route, tags ...string) bool {
	if len(route.Tags) > 0 {
		if len(tags) == 0 {
			return false
		}
		for _, tag := range route.Tags {
			for _, value := range tags {
				if tag != value {
					return false
				}
			}
		}
	}
	return true
}
