// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"fmt"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/sdk/pkg/service"
	"google.golang.org/grpc"
	"os"
)

func newRuntimeService(runtime *Runtime, network Network, options RuntimeServiceOptions) service.Service {
	return &runtimeService{
		RuntimeServiceOptions: options,
		runtime:               runtime,
		network:               network,
		server:                grpc.NewServer(),
	}
}

type runtimeService struct {
	RuntimeServiceOptions
	runtime *Runtime
	network Network
	server  *grpc.Server
}

func (s *runtimeService) Start() error {
	log.Info("Starting primitive service")
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := s.network.Listen(address)
	if err != nil {
		return err
	}

	for _, kind := range s.Types {
		kind.Register(s.server, s.runtime)
	}

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *runtimeService) Stop() error {
	log.Info("Shutting down primitive service")
	s.server.Stop()
	return nil
}

var _ service.Service = (*runtimeService)(nil)

func newProxyService(runtime *Runtime, network Network, options ProxyServiceOptions) service.Service {
	return &proxyService{
		ProxyServiceOptions: options,
		runtime:             runtime,
		network:             network,
		server:              grpc.NewServer(),
	}
}

type proxyService struct {
	ProxyServiceOptions
	runtime *Runtime
	network Network
	server  *grpc.Server
}

func (s *proxyService) Start() error {
	log.Info("Starting proxy service")
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := s.network.Listen(address)
	if err != nil {
		return err
	}

	proxyv1.RegisterProxyServer(s.server, newProxyServer(s.runtime))

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
