// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/logging"
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

	runtimev1.RegisterClusterServiceServer(s.server, newClusterServiceServer(s.runtime.Clusters()))
	runtimev1.RegisterPrimitiveServiceServer(s.server, newPrimitiveServiceServer(s.runtime.Primitives()))
	runtimev1.RegisterBindingServiceServer(s.server, newBindingServiceServer(s.runtime.Bindings()))
	runtimev1.RegisterDriverServiceServer(s.server, newDriverServiceServer(s.runtime.Drivers()))
	runtimev1.RegisterRuntimeServer(s.server, newRuntimeServer(s.runtime))

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
