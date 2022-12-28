// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimeapiv1 "github.com/atomix/atomix/api/runtime/v1"
	runtimev1 "github.com/atomix/atomix/proxy/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	"google.golang.org/grpc"
)

type Service struct {
	network.Service
	Options
}

func NewService(runtime *runtimev1.Runtime, opts ...Option) network.Service {
	var options Options
	options.apply(opts...)
	server := grpc.NewServer(options.GRPCServerOptions...)
	runtimeapiv1.RegisterRuntimeServer(server, runtimev1.NewRuntimeServer(runtime))
	return &Service{
		Options: options,
		Service: network.NewService(server,
			network.WithDriver(options.Network),
			network.WithHost(options.Host),
			network.WithPort(options.Port)),
	}
}
