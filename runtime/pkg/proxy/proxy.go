// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
	"os"
)

var log = logging.GetLogger()

func NewProxy(runtime *runtime.Runtime, opts ...Option) *Proxy {
	var options Options
	options.apply(opts...)
	return &Proxy{
		Options: options,
		runtime: runtime,
		network: options.Network,
		server:  grpc.NewServer(options.GRPCServerOptions...),
	}
}

type Proxy struct {
	Options
	runtime *Runtime
	network Network
	server  *grpc.Server
}

func (s *Proxy) Start() error {
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

func (s *Proxy) Stop() error {
	log.Info("Shutting down runtime controller service")
	s.server.Stop()
	return nil
}

var _ Service = (*Proxy)(nil)
