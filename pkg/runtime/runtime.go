// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"sync"
)

var log = logging.GetLogger()

func New(opts ...Option) *Runtime {
	var options Options
	options.apply(opts...)
	return &Runtime{
		options: options,
		conns:   make(map[runtimev1.PrimitiveId]*runtimeConn),
	}
}

type Runtime struct {
	options        Options
	config         Config
	proxyService   Service
	runtimeService Service
	drivers        *DriverPluginCache
	clusters       Store[runtimev1.ClusterId, *runtimev1.Cluster]
	primitives     Store[runtimev1.PrimitiveId, *runtimev1.Primitive]
	bindings       Store[runtimev1.BindingId, *runtimev1.Binding]
	conns          map[runtimev1.PrimitiveId]*runtimeConn
	mu             sync.RWMutex
}

func (r *Runtime) Start() error {
	// Load the configuration from the configuration file
	bytes, err := ioutil.ReadFile(r.options.ConfigFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Parse the configuration
	if err := yaml.Unmarshal(bytes, &r.config); err != nil {
		return err
	}

	// Initialize the driver plugin cache
	r.drivers = NewDriverPluginCache(r.options.CacheDir)

	// Start the proxy service
	r.proxyService = newProxyService(r, r.options.ProxyServiceOptions)
	if err := r.proxyService.Start(); err != nil {
		return err
	}

	// Start the runtime service
	r.runtimeService = newRuntimeService(r, r.options.RuntimeServiceOptions)
	if err := r.runtimeService.Start(); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) connect(ctx context.Context, primitive runtimev1.Primitive) (driver.Conn, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.conns[primitive.PrimitiveID]
	if !ok {
		return nil, errors.NewUnavailable("connection %s not configured", primitive.PrimitiveID.Name)
	}
	return conn.connect(ctx)
}

func (r *Runtime) addConnection(cluster string, info *runtimev1.ConnectionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.conns[cluster]; ok {
		return errors.NewAlreadyExists("connection %s already exists", cluster)
	}

	driver := r.drivers.Get(info.Driver.Name, info.Driver.Version)
	conn := newRuntimeConn(driver, info.Config)
	r.conns[cluster] = conn
	return nil
}

func (r *Runtime) configureConnection(ctx context.Context, cluster string, info *runtimev1.ConnectionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.conns[cluster]
	if !ok {
		return errors.NewNotFound("connection %s not found", cluster)
	}
	if conn.driver.Name != info.Driver.Name || conn.driver.Version != info.Driver.Version {
		return errors.NewForbidden("driver mismatch with configured connection")
	}
	return conn.configure(ctx, info.Config)
}

func (r *Runtime) closeConnection(ctx context.Context, cluster string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.conns[cluster]
	if !ok {
		return errors.NewNotFound("connection %s not found", cluster)
	}
	delete(r.conns, cluster)
	return conn.disconnect(ctx)
}

func (r *Runtime) Stop() error {
	if err := r.proxyService.Stop(); err != nil {
		return err
	}
	if err := r.runtimeService.Stop(); err != nil {
		return err
	}
	return nil
}

func newRuntimeConn(driver *DriverPlugin, config []byte) *runtimeConn {
	return &runtimeConn{
		driver: driver,
		config: config,
	}
}

type runtimeConn struct {
	driver *DriverPlugin
	config []byte
	conn   driver.Conn
	mu     sync.RWMutex
}

func (c *runtimeConn) connect(ctx context.Context) (driver.Conn, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}

	driver, err := c.driver.Load()
	if err != nil {
		return nil, err
	}

	conn, err = driver.Connect(ctx, c.config)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *runtimeConn) configure(ctx context.Context, config []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		c.config = config
		return nil
	}
	return c.conn.Configure(ctx, config)
}

func (c *runtimeConn) disconnect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.Close(ctx)
}
