// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"fmt"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/runtime"
	"github.com/atomix/runtime/pkg/atomix/service"
	"google.golang.org/grpc"
	"net"
	"os"
)

func New(runtime runtime.Runtime, opts ...Option) *Atomix {
	var options Options
	options.apply(opts...)
	return &Atomix{
		Options: options,
		runtime: runtime,
		server:  grpc.NewServer(),
	}
}

type Atomix struct {
	Options
	runtime runtime.Runtime
	server  *grpc.Server
}

func (a *Atomix) Start() error {
	address := fmt.Sprintf("%s:%d", a.Host, a.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	atomixv1.RegisterAtomixServer(a.server, newServer(a.runtime))
	for _, kind := range a.PrimitiveKinds {
		kind.Register(a.server, a.registry)
	}

	go func() {
		if err := a.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (a *Atomix) Stop() error {
	a.server.Stop()
	return nil
}

var _ service.Service = (*Atomix)(nil)
