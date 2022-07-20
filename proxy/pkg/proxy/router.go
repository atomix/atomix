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
	Store StoreID      `json:"store" yaml:"store"`
	Rules []RuleConfig `json:"rules" yaml:"rules"`
}

type RuleConfig struct {
	Kinds       []string          `json:"kinds" yaml:"kinds"`
	APIVersions []string          `json:"apiVersions" yaml:"apiVersions"`
	Names       []string          `json:"names" yaml:"names"`
	Tags        map[string]string `json:"tags" yaml:"tags"`
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

func (r *Router) Route(primitive runtime.PrimitiveMeta) (StoreID, error) {
	for _, route := range r.routes {
		if route.Matches(primitive) {
			return route.Store, nil
		}
	}
	return StoreID{}, errors.NewForbidden("no route found for '%s'", primitive.Name)
}

func newRoute(binding RouteConfig) *Route {
	rules := make([]*Rule, len(binding.Rules))
	for i, rule := range binding.Rules {
		rules[i] = newRule(rule)
	}
	return &Route{
		RouteConfig: binding,
		rules:       rules,
	}
}

type Route struct {
	RouteConfig
	rules []*Rule
}

func (r *Route) Matches(primitive runtime.PrimitiveMeta) bool {
	for _, rule := range r.rules {
		if rule.Matches(primitive) {
			return true
		}
	}
	return false
}

func newRule(rule RuleConfig) *Rule {
	return &Rule{
		RuleConfig: rule,
	}
}

type Rule struct {
	RuleConfig
}

func (r *Rule) Matches(primitive runtime.PrimitiveMeta) bool {
	if !r.matchesKind(primitive.Kind.Name) {
		return false
	}
	if !r.matchesAPIVersion(primitive.Kind.APIVersion) {
		return false
	}
	if !r.matchesName(primitive.Name) {
		return false
	}
	if !r.matchesTags(primitive.Tags) {
		return false
	}
	return true
}

func (r *Rule) matchesKind(kind string) bool {
	return matches(r.Kinds, kind)
}

func (r *Rule) matchesAPIVersion(version string) bool {
	return matches(r.APIVersions, version)
}

func (r *Rule) matchesName(name string) bool {
	return matches(r.Names, name)
}

func (r *Rule) matchesTags(tags map[string]string) bool {
	for matchKey, matchValue := range r.Tags {
		if tags[matchKey] != matchValue {
			return false
		}
	}
	return true
}

func matches(names []string, name string) bool {
	if len(names) == 0 {
		return true
	}
	for _, match := range names {
		if match == wildcard || match == name {
			return true
		}
	}
	return false
}
