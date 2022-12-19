// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
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
	"os"
)

var log = logging.GetLogger()

func register(proxy *Proxy) {
	counterv1.RegisterServer(proxy.server, proxy.runtime)
	countermapv1.RegisterServer(proxy.server, proxy.runtime)
	electionv1.RegisterServer(proxy.server, proxy.runtime)
	indexedmapv1.RegisterServer(proxy.server, proxy.runtime)
	listv1.RegisterServer(proxy.server, proxy.runtime)
	lockv1.RegisterServer(proxy.server, proxy.runtime)
	mapv1.RegisterServer(proxy.server, proxy.runtime)
	multimapv1.RegisterServer(proxy.server, proxy.runtime)
	setv1.RegisterServer(proxy.server, proxy.runtime)
	topicv1.RegisterServer(proxy.server, proxy.runtime)
	valuev1.RegisterServer(proxy.server, proxy.runtime)
}

func New(runtime runtime.Runtime, opts ...Option) *Proxy {
	var options Options
	options.apply(opts...)
	return &Proxy{
		Options: options,
		runtime: runtime,
		server:  grpc.NewServer(options.GRPCServerOptions...),
	}
}

type Proxy struct {
	Options
	runtime runtime.Runtime
	server  *grpc.Server
}

func (p *Proxy) Start() error {
	log.Info("Starting runtime controller service")
	address := fmt.Sprintf("%s:%d", p.Host, p.Port)
	lis, err := p.Network.Listen(address)
	if err != nil {
		return err
	}

	// Register proxy servers
	register(p)

	go func() {
		if err := p.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (p *Proxy) Stop() error {
	log.Info("Shutting down runtime controller service")
	p.server.Stop()
	return nil
}
