// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"google.golang.org/grpc"
	"os"
)

// Service is an interface for starting and stopping a process
type Service interface {
	Start() error
	Stop() error
}

func NewController(runtime *Runtime, opts ...ControllerOption) *Controller {
	var options ControllerOptions
	options.apply(opts...)
	return &Controller{
		ControllerOptions: options,
		runtime:           runtime,
		network:           options.Network,
		server:            grpc.NewServer(options.GRPCServerOptions...),
	}
}

type Controller struct {
	ControllerOptions
	runtime *Runtime
	network Network
	server  *grpc.Server
}

func (s *Controller) Start() error {
	log.Info("Starting runtime controller service")
	address := fmt.Sprintf("%s:%d", s.Host, s.Port)
	lis, err := s.network.Listen(address)
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

func (s *Controller) Stop() error {
	log.Info("Shutting down runtime controller service")
	s.server.Stop()
	return nil
}

var _ Service = (*Controller)(nil)
