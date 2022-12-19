// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/atomix/runtime/pkg/network"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	counterv1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	countermapv1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	electionv1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	indexedmapv1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	listv1 "github.com/atomix/atomix/runtime/pkg/runtime/list/v1"
	lockv1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	mapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	multimapv1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	setv1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	topicv1 "github.com/atomix/atomix/runtime/pkg/runtime/topic/v1"
	valuev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
	"google.golang.org/grpc"
)

func register(server *grpc.Server, runtime runtime.Runtime) {
	counterv1.RegisterServer(server, runtime)
	countermapv1.RegisterServer(server, runtime)
	electionv1.RegisterServer(server, runtime)
	indexedmapv1.RegisterServer(server, runtime)
	listv1.RegisterServer(server, runtime)
	lockv1.RegisterServer(server, runtime)
	mapv1.RegisterServer(server, runtime)
	multimapv1.RegisterServer(server, runtime)
	setv1.RegisterServer(server, runtime)
	topicv1.RegisterServer(server, runtime)
	valuev1.RegisterServer(server, runtime)
}

type Proxy interface {
	network.Service
}

func New(runtime runtime.Runtime, opts ...Option) Proxy {
	var options Options
	options.apply(opts...)
	p := &proxy{
		Options: options,
	}

	server := grpc.NewServer(options.GRPCServerOptions...)
	register(server, runtime)

	p.Service = network.NewService(server,
		network.WithDriver(network.NewDefaultDriver()),
		network.WithHost(options.Host),
		network.WithPort(options.Port))
	return p
}

type proxy struct {
	network.Service
	Options
}
