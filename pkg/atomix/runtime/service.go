// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/service"
	"google.golang.org/grpc"
	"os"
)

func newProxyService(runtime *Runtime, options ProxyServiceOptions) service.Service {
	return &proxyService{
		ProxyServiceOptions: options,
		runtime:             runtime,
		server:              grpc.NewServer(),
	}
}

type proxyService struct {
	ProxyServiceOptions
	runtime *Runtime
	server  *grpc.Server
}

func (s *proxyService) Start() error {
	log.Info("Starting proxy service")
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := s.runtime.network.Listen(address)
	if err != nil {
		return err
	}

	client := newClient(s.runtime)
	for _, kind := range s.Kinds {
		kind.Register(s.server, client)
	}

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *proxyService) Stop() error {
	log.Info("Shutting down proxy service")
	s.server.Stop()
	return nil
}

var _ service.Service = (*proxyService)(nil)

func newControlService(runtime *Runtime, options ControlServiceOptions) service.Service {
	return &controlService{
		ControlServiceOptions: options,
		runtime:               runtime,
		server:                grpc.NewServer(),
	}
}

type controlService struct {
	ControlServiceOptions
	runtime *Runtime
	server  *grpc.Server
}

func (s *controlService) Start() error {
	log.Info("Starting control service")
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := s.runtime.network.Listen(address)
	if err != nil {
		return err
	}

	runtimev1.RegisterRuntimeServer(s.server, newRuntimeServer(s.runtime))
	runtimev1.RegisterPrimitiveServiceServer(s.server, newPrimitiveServiceServer(s.runtime.primitives))
	runtimev1.RegisterBindingServiceServer(s.server, newBindingServiceServer(s.runtime.bindings))
	runtimev1.RegisterClusterServiceServer(s.server, newClusterServiceServer(s.runtime.clusters))
	runtimev1.RegisterDriverServiceServer(s.server, newDriverServiceServer(s.runtime.drivers.plugins))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *controlService) Stop() error {
	log.Info("Shutting down control service")
	s.server.Stop()
	return nil
}

var _ service.Service = (*controlService)(nil)
