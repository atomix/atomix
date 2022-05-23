// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime/plugin"
	"github.com/atomix/runtime/pkg/runtime/store"
	"github.com/atomix/runtime/version"
)

var log = logging.GetLogger()

type Version string

type Runtime interface {
	Version() Version
	Clusters() store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
	Proxies() store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]
	Bindings() store.Store[*runtimev1.BindingId, *runtimev1.Binding]
	Drivers() store.Store[*runtimev1.DriverId, *runtimev1.Driver]
	Plugins() *plugin.Cache[driver.Driver]
	Connect(ctx context.Context, proxy *runtimev1.Proxy) (driver.Conn, error)
}

func New(opts ...Option) Runtime {
	var options Options
	options.apply(opts...)
	return &atomixRuntime{
		clusters: store.NewStore[*runtimev1.ClusterId, *runtimev1.Cluster](),
		proxies:  store.NewStore[*runtimev1.ProxyId, *runtimev1.Proxy](),
		bindings: store.NewStore[*runtimev1.BindingId, *runtimev1.Binding](),
		drivers:  store.NewStore[*runtimev1.DriverId, *runtimev1.Driver](),
		plugins:  plugin.NewCache[driver.Driver](options.CacheDir),
	}
}

type atomixRuntime struct {
	clusters store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
	proxies  store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]
	bindings store.Store[*runtimev1.BindingId, *runtimev1.Binding]
	drivers  store.Store[*runtimev1.DriverId, *runtimev1.Driver]
	plugins  *plugin.Cache[driver.Driver]
}

func (r *atomixRuntime) Version() Version {
	return Version(version.Version())
}

func (r *atomixRuntime) Clusters() store.Store[*runtimev1.ClusterId, *runtimev1.Cluster] {
	return r.clusters
}

func (r *atomixRuntime) Proxies() store.Store[*runtimev1.ProxyId, *runtimev1.Proxy] {
	return r.proxies
}

func (r *atomixRuntime) Bindings() store.Store[*runtimev1.BindingId, *runtimev1.Binding] {
	return r.bindings
}

func (r *atomixRuntime) Drivers() store.Store[*runtimev1.DriverId, *runtimev1.Driver] {
	return r.drivers
}

func (r *atomixRuntime) Plugins() *plugin.Cache[driver.Driver] {
	return r.plugins
}

func (r *atomixRuntime) Connect(ctx context.Context, proxy *runtimev1.Proxy) (driver.Conn, error) {
	//TODO implement me
	panic("implement me")
}

var _ Runtime = (*atomixRuntime)(nil)
