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
	"os"
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
			Namespace: os.Getenv("NAMESPACE"),
			Name:      id.Application,
		}
		application, ok := c.runtime.applications.Get(applicationID)
		if !ok {
			return nil, errors.NewUnavailable("application %s not found", applicationID)
		}

		var clusterID *runtimev1.ClusterId
		for _, binding := range application.Spec.Bindings {
			var matches bool
			for _, rule := range binding.Rules {
				nameMatches := false
				for _, name := range rule.Names {
					if name == id.Primitive {
						nameMatches = true
						break
					}
				}
				if nameMatches {
					matches = true
					break
				}

				md, _ := metadata.FromIncomingContext(ctx)
				headersMatch := true
				for key, value := range rule.Headers {
					headerMatches := false
					for _, v := range md[key] {
						if v == value {
							headerMatches = true
							break
						}
					}
					if !headerMatches {
						headersMatch = false
						break
					}
				}
				if headersMatch {
					matches = true
					break
				}
			}
			if matches {
				clusterID = &binding.ClusterID
				break
			}
		}

		primitive = &runtimev1.Primitive{
			PrimitiveMeta: runtimev1.PrimitiveMeta{
				ID: *primitiveID,
			},
			Spec: runtimev1.PrimitiveSpec{
				Cluster: *clusterID,
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
