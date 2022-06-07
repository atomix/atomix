// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"github.com/atomix/runtime/pkg/atomix/service"
	"google.golang.org/grpc"
	"net"
	"os"
)

func newPrimitiveService(client primitive.Client, options PrimitiveServiceOptions) service.Service {
	return &primitiveService{
		PrimitiveServiceOptions: options,
		client:                  client,
		server:                  grpc.NewServer(),
	}
}

type primitiveService struct {
	PrimitiveServiceOptions
	client primitive.Client
	server *grpc.Server
}

func (s *primitiveService) Start() error {
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for _, kind := range s.Kinds {
		kind.Register(s.server, s.client)
	}

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *primitiveService) Stop() error {
	s.server.Stop()
	return nil
}

var _ service.Service = (*primitiveService)(nil)

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
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	runtimev1.RegisterRuntimeServer(s.server, newRuntimeServer(s.runtime))
	runtimev1.RegisterPrimitiveServiceServer(s.server, newPrimitiveServiceServer(s.runtime.primitives))
	runtimev1.RegisterApplicationServiceServer(s.server, newApplicationServiceServer(s.runtime.applications))
	runtimev1.RegisterClusterServiceServer(s.server, newClusterServiceServer(s.runtime.clusters))
	runtimev1.RegisterDriverServiceServer(s.server, newDriverServiceServer(s.runtime.drivers))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (s *controlService) Stop() error {
	s.server.Stop()
	return nil
}

var _ service.Service = (*controlService)(nil)
