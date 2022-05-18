// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"google.golang.org/grpc"
	"net"
	"os"
)

type Service interface {
	Start() error
	Stop() error
}

func newProxyService(runtime *Runtime, options ProxyServiceOptions) Service {
	return &proxyService{
		runtime: runtime,
		options: options,
		server:  grpc.NewServer(),
	}
}

type proxyService struct {
	runtime *Runtime
	options ProxyServiceOptions
	server  *grpc.Server
}

func (s *proxyService) Start() error {
	address := fmt.Sprintf("%s:%d", s.options.ProxyHost, s.options.ProxyPort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for _, primitive := range s.options.Primitives {
		primitive.Register(s.server, s.runtime.connect)
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
	s.server.Stop()
	return nil
}

func newRuntimeService(runtime *Runtime, options RuntimeServiceOptions) Service {
	return &runtimeService{
		runtime: runtime,
		options: options,
		server:  grpc.NewServer(),
	}
}

type runtimeService struct {
	runtime *Runtime
	options RuntimeServiceOptions
	server  *grpc.Server
}

func (s *runtimeService) Start() error {
	address := fmt.Sprintf("%s:%d", s.options.RuntimeHost, s.options.RuntimePort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	runtimev1.RegisterRuntimeServer(s.server, newRuntimeServer(s.runtime))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *runtimeService) Stop() error {
	s.server.Stop()
	return nil
}
