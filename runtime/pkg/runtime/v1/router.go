// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/gogo/protobuf/types"
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

func (r *router) route(ctx context.Context, meta runtimev1.PrimitiveMeta) (runtimev1.StoreID, *types.Any, error) {
	routes, err := r.provider.LoadRoutes(ctx)
	if err != nil {
		return runtimev1.StoreID{}, nil, errors.NewInternal(err.Error())
	}

	tags := make(map[string]bool)
	for _, tag := range meta.Tags {
		tags[tag] = true
	}

	for _, route := range routes {
		if r.routeMatches(route, tags) {
			for _, primitive := range route.Primitives {
				if r.primitiveMatches(primitive.PrimitiveMeta, meta, tags) {
					return route.StoreID, primitive.Spec, nil
				}
			}
			return route.StoreID, nil, err
		}
	}
	return runtimev1.StoreID{}, nil, errors.NewForbidden("no route matching tags %s", tags)
}

func (r *router) routeMatches(route *runtimev1.Route, tags map[string]bool) bool {
	return tagsMatch(route.Tags, tags)
}

func (r *router) primitiveMatches(primitive runtimev1.PrimitiveMeta, meta runtimev1.PrimitiveMeta, tags map[string]bool) bool {
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
