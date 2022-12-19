// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
	"os"
)

var log = logging.GetLogger()

func New(runtime runtime.Runtime, opts ...Option) *Controller {
	var options Options
	options.apply(opts...)
	return &Controller{
		Options: options,
		runtime: runtime,
		server:  grpc.NewServer(options.GRPCServerOptions...),
	}
}

type Controller struct {
	Options
	runtime runtime.Runtime
	server  *grpc.Server
}

func (c *Controller) Start() error {
	log.Info("Starting runtime controller service")
	address := fmt.Sprintf("%s:%d", c.Host, c.Port)
	lis, err := c.Network.Listen(address)
	if err != nil {
		return err
	}

	runtimev1.RegisterRuntimeServer(c.server, newRuntimeServer(c.runtime))

	go func() {
		if err := c.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (c *Controller) Stop() error {
	log.Info("Shutting down runtime controller service")
	c.server.Stop()
	return nil
}
