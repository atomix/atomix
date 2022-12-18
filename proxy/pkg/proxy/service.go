// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"fmt"
	proxyv1 "github.com/atomix/atomix/api/pkg/proxy/v1"
	"github.com/atomix/atomix/common/pkg/network"
	"github.com/atomix/atomix/common/pkg/service"
	"google.golang.org/grpc"
	"os"
)

func newProxyService(runtime *Runtime, network network.Network, config ServerConfig, options ProxyServiceOptions) service.Service {
	var opts []grpc.ServerOption
	if config.ReadBufferSize != nil {
		opts = append(opts, grpc.ReadBufferSize(*config.ReadBufferSize))
	}
	if config.WriteBufferSize != nil {
		opts = append(opts, grpc.WriteBufferSize(*config.WriteBufferSize))
	}
	if config.MaxSendMsgSize != nil {
		opts = append(opts, grpc.MaxSendMsgSize(*config.MaxSendMsgSize))
	}
	if config.MaxRecvMsgSize != nil {
		opts = append(opts, grpc.MaxRecvMsgSize(*config.MaxRecvMsgSize))
	}
	if config.NumStreamWorkers != nil {
		opts = append(opts, grpc.NumStreamWorkers(*config.NumStreamWorkers))
	}
	if config.MaxConcurrentStreams != nil {
		opts = append(opts, grpc.MaxConcurrentStreams(*config.MaxConcurrentStreams))
	}
	return &proxyService{
		ProxyServiceOptions: options,
		runtime:             runtime,
		network:             network,
		server:              grpc.NewServer(opts...),
	}
}

type proxyService struct {
	ProxyServiceOptions
	runtime *Runtime
	network network.Network
	server  *grpc.Server
}

func (s *proxyService) Start() error {
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

func (s *proxyService) Stop() error {
	log.Info("Shutting down primitive service")
	s.server.Stop()
	return nil
}

var _ service.Service = (*proxyService)(nil)

func newProxyControlService(runtime *Runtime, network network.Network, options ProxyControlServiceOptions) service.Service {
	return &proxyControlService{
		ProxyControlServiceOptions: options,
		runtime:                    runtime,
		network:                    network,
		server:                     grpc.NewServer(),
	}
}

type proxyControlService struct {
	ProxyControlServiceOptions
	runtime *Runtime
	network network.Network
	server  *grpc.Server
}

func (s *proxyControlService) Start() error {
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

func (s *proxyControlService) Stop() error {
	log.Info("Shutting down proxy service")
	s.server.Stop()
	return nil
}

var _ service.Service = (*proxyControlService)(nil)
