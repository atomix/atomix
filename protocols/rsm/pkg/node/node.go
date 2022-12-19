// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"fmt"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/network"
	"google.golang.org/grpc"
	"os"
)

var log = logging.GetLogger()

func NewNode(network network.Driver, protocol Protocol, opts ...Option) *Node {
	var options Options
	options.apply(opts...)
	return &Node{
		Options:  options,
		Protocol: protocol,
		network:  network,
		server:   grpc.NewServer(options.GRPCServerOptions...),
	}
}

type Service func(*grpc.Server)

type Node struct {
	Options
	Protocol
	network  network.Driver
	server   *grpc.Server
	services []Service
}

func (n *Node) RegisterService(service Service) {
	n.services = append(n.services, service)
}

func (n *Node) Start() error {
	log.Infow("Starting Node")
	address := fmt.Sprintf("%s:%d", n.Host, n.Port)
	lis, err := n.network.Listen(address)
	if err != nil {
		log.Errorw("Error starting Node",
			logging.Error("Error", err))
		return err
	}

	server := newServer(n)
	protocol.RegisterPartitionServer(n.server, server)
	protocol.RegisterSessionServer(n.server, server)

	for _, service := range n.services {
		service(n.server)
	}

	go func() {
		if err := n.server.Serve(lis); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()
	return nil
}

func (n *Node) Stop() error {
	log.Infow("Stopping Node")
	n.server.Stop()
	return nil
}
