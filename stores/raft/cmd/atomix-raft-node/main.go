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
	counterv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/counter/v1"
	countermapv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/countermap/v1"
	electionv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/election/v1"
	indexedmapv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/indexedmap/v1"
	lockv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/lock/v1"
	mapv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/map/v1"
	multimapv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/multimap/v1"
	setv1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/set/v1"
	valuev1 "github.com/atomix/atomix/protocols/rsm/pkg/statemachine/value/v1"
	"github.com/atomix/atomix/runtime/pkg/network"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
	raft "github.com/atomix/atomix/stores/raft/pkg/node"
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
		Use: "atomix-raft-node",
		Run: func(cmd *cobra.Command, args []string) {
			configPath, err := cmd.Flags().GetString("config")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			apiHost, err := cmd.Flags().GetString("api-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			apiPort, err := cmd.Flags().GetInt("api-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			raftHost, err := cmd.Flags().GetString("raft-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			raftPort, err := cmd.Flags().GetInt("raft-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			config := raft.Config{}
			configBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := yaml.Unmarshal(configBytes, &config); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			registry := statemachine.NewPrimitiveTypeRegistry()
			counterv1.RegisterStateMachine(registry)
			countermapv1.RegisterStateMachine(registry)
			electionv1.RegisterStateMachine(registry)
			indexedmapv1.RegisterStateMachine(registry)
			lockv1.RegisterStateMachine(registry)
			mapv1.RegisterStateMachine(registry)
			multimapv1.RegisterStateMachine(registry)
			setv1.RegisterStateMachine(registry)
			valuev1.RegisterStateMachine(registry)

			protocol := raft.NewProtocol(
				config.Raft,
				registry,
				raft.WithHost(raftHost),
				raft.WithPort(raftPort))

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

			node := node.NewNode(
				network.NewDefaultDriver(),
				protocol,
				node.WithHost(apiHost),
				node.WithPort(apiPort),
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
			node.RegisterService(func(server *grpc.Server) {
				raftv1.RegisterNodeServer(server, raft.NewNodeServer(protocol))
			})

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
	cmd.Flags().StringP("config", "c", "", "the path to the raft cluster configuration")
	cmd.Flags().String("api-host", "", "the host to which to bind the API server")
	cmd.Flags().Int("api-port", 8080, "the port to which to bind the API server")
	cmd.Flags().String("raft-host", "", "the host to which to bind the Multi-Raft server")
	cmd.Flags().Int("raft-port", 5000, "the port to which to bind the Multi-Raft server")

	_ = cmd.MarkFlagRequired("node")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
