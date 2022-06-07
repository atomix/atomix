// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc/metadata"
	"sync"
)

const wildcard = "*"

func newClient(runtime *Runtime) primitive.Client {
	return &runtimeClient{
		runtime: runtime,
		conns:   make(map[runtimev1.ClusterId]driver.Conn),
	}
}

type runtimeClient struct {
	runtime *Runtime
	conns   map[runtimev1.ClusterId]driver.Conn
	mu      sync.RWMutex
}

func (c *runtimeClient) Connect(ctx context.Context, id primitive.ID) (driver.Conn, error) {
	primitiveID := &runtimev1.PrimitiveId{
		Application: id.Application,
		Primitive:   id.Primitive,
		Session:     id.Session,
	}
	primitive, ok := c.runtime.primitives.Get(primitiveID)
	if !ok {
		applicationID := &runtimev1.ApplicationId{
			Namespace: c.runtime.Namespace,
			Name:      id.Application,
		}
		application, ok := c.runtime.applications.Get(applicationID)
		if !ok {
			return nil, errors.NewUnavailable("application %s not found", applicationID)
		}

		clusterID, ok := c.getClusterID(ctx, application, id)
		if !ok {
			return nil, errors.NewUnavailable("cluster binding not found in application %s", applicationID)
		}

		primitive = &runtimev1.Primitive{
			PrimitiveMeta: runtimev1.PrimitiveMeta{
				ID: *primitiveID,
			},
			Spec: runtimev1.PrimitiveSpec{
				Cluster: clusterID,
			},
		}
		if err := c.runtime.primitives.Create(primitive); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	conn, ok := c.conns[primitive.Spec.Cluster]
	if !ok {
		cluster, ok := c.runtime.clusters.Get(&primitive.Spec.Cluster)
		if !ok {
			return nil, errors.NewUnavailable("cluster %s not found", primitive.Spec.Cluster)
		}

		driver, err := c.runtime.drivers.get(cluster.Spec.Driver.Name, cluster.Spec.Driver.Version)
		if err != nil {
			return nil, err
		}

		conn, err := driver.Connect(ctx, cluster.Spec.Config)
		if err != nil {
			return nil, err
		}
		c.conns[primitive.Spec.Cluster] = conn
	}
	return conn, nil
}

func (c *runtimeClient) getClusterID(ctx context.Context, app *runtimev1.Application, id primitive.ID) (runtimev1.ClusterId, bool) {
	for _, binding := range app.Spec.Bindings {
		for _, rule := range binding.Rules {
			if c.isRuleMatch(ctx, id, rule) {
				return binding.ClusterID, true
			}
		}
	}
	return runtimev1.ClusterId{}, false
}

func (c *runtimeClient) isRuleMatch(ctx context.Context, id primitive.ID, rule runtimev1.BindingRule) bool {
	if rule.Kinds == nil && rule.Names == nil && rule.Headers == nil {
		return false
	}
	return c.isKindMatch(id, rule) && c.isNameMatch(id, rule) && c.isHeadersMatch(ctx, id, rule)
}

func (c *runtimeClient) isKindMatch(id primitive.ID, rule runtimev1.BindingRule) bool {
	if rule.Kinds == nil {
		return true
	}

	for _, kind := range rule.Kinds {
		if kind == wildcard || kind == id.Service {
			return true
		}
	}
	return false
}

func (c *runtimeClient) isNameMatch(id primitive.ID, rule runtimev1.BindingRule) bool {
	if rule.Names == nil {
		return true
	}

	for _, name := range rule.Names {
		if name == wildcard || name == id.Primitive {
			return true
		}
	}
	return false
}

func (c *runtimeClient) isHeadersMatch(ctx context.Context, id primitive.ID, rule runtimev1.BindingRule) bool {
	if rule.Headers == nil {
		return true
	}

	md, _ := metadata.FromIncomingContext(ctx)
headers:
	for key, value := range rule.Headers {
		for _, v := range md.Get(key) {
			if value == wildcard || v == value {
				continue headers
			}
		}
		return false
	}
	return true
}

func (c *runtimeClient) Close(ctx context.Context, id primitive.ID) error {
	primitiveID := &runtimev1.PrimitiveId{
		Application: id.Application,
		Primitive:   id.Primitive,
		Session:     id.Session,
	}
	primitive, ok := c.runtime.primitives.Get(primitiveID)
	if !ok {
		return nil
	}
	if err := c.runtime.primitives.Delete(primitive); err != nil {
		return err
	}
	return nil
}

var _ primitive.Client = (*runtimeClient)(nil)
