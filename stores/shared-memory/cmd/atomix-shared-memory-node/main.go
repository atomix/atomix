// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	counternodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/counter/v1"
	countermapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/countermap/v1"
	electionnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/election/v1"
	indexedmapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/indexedmap/v1"
	locknodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/lock/v1"
	mapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/map/v1"
	multimapnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/multimap/v1"
	setnodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/set/v1"
	valuenodev1 "github.com/atomix/atomix/protocols/rsm/pkg/node/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	counterstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/counter/v1"
	countermapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/countermap/v1"
	electionstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/election/v1"
	indexedmapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/indexedmap/v1"
	lockstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/lock/v1"
	mapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/map/v1"
	multimapstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/multimap/v1"
	setstatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/set/v1"
	valuestatemachinev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/value/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	sharedmemory "github.com/atomix/atomix/stores/shared-memory/pkg/node"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-shared-memory-node",
		Run: func(cmd *cobra.Command, args []string) {
			configPath, err := cmd.Flags().GetString("config")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			host, err := cmd.Flags().GetString("host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			config := sharedmemory.Config{}
			configBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := yaml.Unmarshal(configBytes, &config); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			var serverOptions []grpc.ServerOption
			if config.Server.ReadBufferSize != nil {
				serverOptions = append(serverOptions, grpc.ReadBufferSize(*config.Server.ReadBufferSize))
			}
			if config.Server.WriteBufferSize != nil {
				serverOptions = append(serverOptions, grpc.WriteBufferSize(*config.Server.WriteBufferSize))
			}
			if config.Server.MaxSendMsgSize != nil {
				serverOptions = append(serverOptions, grpc.MaxSendMsgSize(*config.Server.MaxSendMsgSize))
			}
			if config.Server.MaxRecvMsgSize != nil {
				serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(*config.Server.MaxRecvMsgSize))
			}
			if config.Server.NumStreamWorkers != nil {
				serverOptions = append(serverOptions, grpc.NumStreamWorkers(*config.Server.NumStreamWorkers))
			}
			if config.Server.MaxConcurrentStreams != nil {
				serverOptions = append(serverOptions, grpc.MaxConcurrentStreams(*config.Server.MaxConcurrentStreams))
			}

			registry := statemachine.NewPrimitiveTypeRegistry()
			counterstatemachinev1.RegisterStateMachine(registry)
			countermapstatemachinev1.RegisterStateMachine(registry)
			electionstatemachinev1.RegisterStateMachine(registry)
			indexedmapstatemachinev1.RegisterStateMachine(registry)
			lockstatemachinev1.RegisterStateMachine(registry)
			mapstatemachinev1.RegisterStateMachine(registry)
			multimapstatemachinev1.RegisterStateMachine(registry)
			setstatemachinev1.RegisterStateMachine(registry)
			valuestatemachinev1.RegisterStateMachine(registry)

			node := node.NewNode(
				network.NewDefaultDriver(),
				sharedmemory.NewProtocol(registry),
				node.WithHost(host),
				node.WithPort(port),
				node.WithGRPCServerOptions(serverOptions...))

			counternodev1.RegisterServer(node)
			countermapnodev1.RegisterServer(node)
			electionnodev1.RegisterServer(node)
			indexedmapnodev1.RegisterServer(node)
			locknodev1.RegisterServer(node)
			mapnodev1.RegisterServer(node)
			multimapnodev1.RegisterServer(node)
			setnodev1.RegisterServer(node)
			valuenodev1.RegisterServer(node)

			// Start the node
			if err := node.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the node
			if err := node.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("config", "c", "", "the path to the node configuration")
	cmd.Flags().String("host", "", "the host to which to bind the server")
	cmd.Flags().Int("port", 8080, "the port to which to bind the server")

	_ = cmd.MarkFlagRequired("node")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
