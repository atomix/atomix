// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/binding"
	"github.com/atomix/runtime/pkg/atomix/runtime/cluster"
	"github.com/atomix/runtime/pkg/atomix/runtime/driver"
	"github.com/atomix/runtime/pkg/atomix/runtime/proxy"
	"github.com/atomix/runtime/pkg/atomix/runtime/session"
	"github.com/atomix/runtime/pkg/atomix/service"
	"github.com/atomix/runtime/pkg/atomix/store"
	"github.com/atomix/runtime/version"
	"google.golang.org/grpc"
	"net"
	"os"
)

var log = logging.GetLogger()

type Version string

type Runtime interface {
	Version() Version
	OpenSession(session *runtimev1.Session) error
	CloseSession(sessionID runtimev1.SessionId) error
}

type Service interface {
	Runtime
	service.Service
}

func New(opts ...Option) Service {
	var options Options
	options.apply(opts...)
	return &runtimeService{
		options: options,
		server:  grpc.NewServer(),
	}
}

type runtimeService struct {
	options  Options
	server   *grpc.Server
	proxies  proxy.Service
	sessions session.Service
	bindings binding.Service
	clusters cluster.Service
	drivers  driver.Service
}

func (r *runtimeService) Version() Version {
	return Version(version.Version())
}

func (r *runtimeService) Start() error {
	if err := r.startServices(); err != nil {
		return err
	}
	if err := r.startServer(); err != nil {
		return err
	}
	return nil
}

func (r *runtimeService) startServices() error {
	r.drivers = driver.NewService(store.NewStore[*runtimev1.DriverId, *runtimev1.Driver]())
	if err := r.drivers.Start(); err != nil {
		return err
	}
	r.clusters = cluster.NewService(store.NewStore[*runtimev1.ClusterId, *runtimev1.Cluster]())
	if err := r.clusters.Start(); err != nil {
		return err
	}
	r.bindings = binding.NewService(store.NewStore[*runtimev1.BindingId, *runtimev1.Binding]())
	if err := r.bindings.Start(); err != nil {
		return err
	}
	r.sessions = session.NewService(store.NewStore[*runtimev1.SessionId, *runtimev1.Session]())
	if err := r.sessions.Start(); err != nil {
		return err
	}
	r.proxies = proxy.NewService(store.NewStore[*runtimev1.ProxyId, *runtimev1.Proxy]())
	if err := r.proxies.Start(); err != nil {
		return err
	}
	return nil
}

func (r *runtimeService) startServer() error {
	address := fmt.Sprintf("%s:%d", r.options.Host, r.options.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	runtimev1.RegisterRuntimeServer(r.server, newServer(r))
	runtimev1.RegisterProxyServiceServer(r.server, proxy.NewServer(r.proxies))
	runtimev1.RegisterSessionServiceServer(r.server, session.NewServer(r.sessions))
	runtimev1.RegisterBindingServiceServer(r.server, binding.NewServer(r.bindings))
	runtimev1.RegisterClusterServiceServer(r.server, cluster.NewServer(r.clusters))
	runtimev1.RegisterDriverServiceServer(r.server, driver.NewServer(r.drivers))

	go func() {
		if err := r.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (r *runtimeService) Stop() error {
	if err := r.stopServer(); err != nil {
		return err
	}
	if err := r.stopServices(); err != nil {
		return err
	}
	return nil
}

func (r *runtimeService) stopServer() error {
	r.server.Stop()
	return nil
}

func (r *runtimeService) stopServices() error {
	if err := r.proxies.Stop(); err != nil {
		return err
	}
	if err := r.sessions.Stop(); err != nil {
		return err
	}
	if err := r.bindings.Stop(); err != nil {
		return err
	}
	if err := r.clusters.Stop(); err != nil {
		return err
	}
	if err := r.drivers.Stop(); err != nil {
		return err
	}
	return nil
}

var _ Runtime = (*runtimeService)(nil)
