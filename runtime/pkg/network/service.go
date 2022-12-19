// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package network

import (
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
	"os"
)

var log = logging.GetLogger()

type Service interface {
	Start() error
	Stop() error
}

func NewService(server *grpc.Server, opts ...Option) Service {
	var options Options
	options.apply(opts...)
	return &grpcService{
		Options: options,
		server:  server,
	}
}

type grpcService struct {
	Options
	server *grpc.Server
}

func (p *grpcService) Start() error {
	log.Info("Starting service")
	address := fmt.Sprintf("%s:%d", p.Host, p.Port)
	lis, err := p.Network.Listen(address)
	if err != nil {
		return err
	}

	go func() {
		if err := p.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (p *grpcService) Stop() error {
	log.Info("Stopping service")
	p.server.Stop()
	return nil
}
