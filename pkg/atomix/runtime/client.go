// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/env"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc/metadata"
	"sync"
)

func newClient(runtime *Runtime) primitive.Client {
	return &runtimeClient{
		runtime: runtime,
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
			Namespace: env.GetNamespace(),
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

		driver, err := c.runtime.drivers.Get(cluster.Spec.Driver.Name, cluster.Spec.Driver.Version).Load()
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
			for _, name := range rule.Names {
				if name == id.Primitive {
					return binding.ClusterID, true
				}
			}

			md, _ := metadata.FromIncomingContext(ctx)
			for key, value := range rule.Headers {
				for _, v := range md[key] {
					if v == value {
						return binding.ClusterID, true
					}
				}
			}
		}
	}
	return runtimev1.ClusterId{}, false
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
