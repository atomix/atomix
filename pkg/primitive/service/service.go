// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/service"
	"google.golang.org/grpc"
	"net"
	"os"
)

var log = logging.GetLogger()

func NewService(runtime runtime.Runtime, opts ...Option) service.Service {
	var options Options
	options.apply(opts...)
	return &Service{
		Options: options,
		runtime: runtime,
		server:  grpc.NewServer(),
	}
}

type Service struct {
	Options
	runtime runtime.Runtime
	server  *grpc.Server
}

func (s *Service) Start() error {
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	registry := primitive.NewRegistry()
	primitivev1.RegisterSessionServiceServer(s.server, newSessionServiceServer(s.runtime, registry, s.PrimitiveTypes...))
	for _, primitiveType := range s.PrimitiveTypes {
		primitiveType.Register(s.server, registry)
	}

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *Service) Stop() error {
	s.server.Stop()
	return nil
}
