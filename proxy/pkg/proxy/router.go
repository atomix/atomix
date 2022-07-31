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
	Store    StoreID         `json:"store" yaml:"store"`
	Bindings []BindingConfig `json:"bindings" yaml:"bindings"`
}

type BindingConfig struct {
	Services   []ServiceConfig   `json:"services" yaml:"services"`
	MatchRules []MatchRuleConfig `json:"matchRules" yaml:"matchRules"`
}

type ServiceConfig struct {
	Name   string                 `json:"name" yaml:"name"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

type MatchRuleConfig struct {
	Names []string          `json:"names" yaml:"names"`
	Tags  map[string]string `json:"tags" yaml:"tags"`
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
	bindings := make([]*Binding, len(route.Bindings))
	for i, binding := range route.Bindings {
		bindings[i] = newBinding(binding)
	}
	return &Route{
		RouteConfig: route,
		bindings:    bindings,
	}
}

type Route struct {
	RouteConfig
	bindings []*Binding
}

func (r *Route) GetConfig(primitive runtime.PrimitiveMeta) (map[string]interface{}, bool) {
	for _, binding := range r.bindings {
		if config, ok := binding.GetConfig(primitive); ok {
			return config, true
		}
	}
	return nil, false
}

func newBinding(config BindingConfig) *Binding {
	rules := make([]*MatchRule, len(config.MatchRules))
	for i, rule := range config.MatchRules {
		rules[i] = newMatchRule(rule)
	}
	return &Binding{
		BindingConfig: config,
		rules:         rules,
	}
}

type Binding struct {
	BindingConfig
	rules []*MatchRule
}

func (b *Binding) GetConfig(primitive runtime.PrimitiveMeta) (map[string]interface{}, bool) {
	for _, rule := range b.rules {
		if rule.Matches(primitive) {
			config := make(map[string]interface{})
			for _, service := range b.Services {
				if service.Name == primitive.Service {
					if service.Config != nil {
						config = service.Config
						break
					}
				}
			}
			return config, true
		}
	}
	return nil, false
}

func newMatchRule(config MatchRuleConfig) *MatchRule {
	return &MatchRule{
		MatchRuleConfig: config,
	}
}

type MatchRule struct {
	MatchRuleConfig
}

func (r *MatchRule) Matches(primitive runtime.PrimitiveMeta) bool {
	if !r.matchesName(primitive.Name) {
		return false
	}
	if !r.matchesTags(primitive.Tags) {
		return false
	}
	return true
}

func (r *MatchRule) matchesName(name string) bool {
	return matches(r.Names, name)
}

func (r *MatchRule) matchesTags(tags map[string]string) bool {
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
