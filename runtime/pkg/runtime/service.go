// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimeapiv1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	runtimev1 "github.com/atomix/atomix/runtime/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	"google.golang.org/grpc"
)

type Service struct {
	network.Service
	Options
}

func NewService(runtime *runtimev1.Runtime, opts ...Option) network.Service {
	var options Options
	options.apply(opts...)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptors.ErrorHandlingUnaryServerInterceptor()),
		grpc.StreamInterceptor(interceptors.ErrorHandlingStreamServerInterceptor()))
	runtimeapiv1.RegisterRuntimeServer(server, runtimev1.NewRuntimeServer(runtime))
	return &Service{
		Options: options,
		Service: network.NewService(server,
			network.WithDriver(options.Network),
			network.WithHost(options.Host),
			network.WithPort(options.Port)),
	}
}
