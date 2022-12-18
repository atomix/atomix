// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/atomix/common/pkg/network"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	counterv1 "github.com/atomix/atomix/proxy/pkg/proxy/counter/v1"
	countermapv1 "github.com/atomix/atomix/proxy/pkg/proxy/countermap/v1"
	electionv1 "github.com/atomix/atomix/proxy/pkg/proxy/election/v1"
	indexedmapv1 "github.com/atomix/atomix/proxy/pkg/proxy/indexedmap/v1"
	listv1 "github.com/atomix/atomix/proxy/pkg/proxy/list/v1"
	lockv1 "github.com/atomix/atomix/proxy/pkg/proxy/lock/v1"
	mapv1 "github.com/atomix/atomix/proxy/pkg/proxy/map/v1"
	multimapv1 "github.com/atomix/atomix/proxy/pkg/proxy/multimap/v1"
	setv1 "github.com/atomix/atomix/proxy/pkg/proxy/set/v1"
	topicv1 "github.com/atomix/atomix/proxy/pkg/proxy/topic/v1"
	valuev1 "github.com/atomix/atomix/proxy/pkg/proxy/value/v1"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-proxy",
		Run: func(cmd *cobra.Command, args []string) {
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
			controlHost, err := cmd.Flags().GetString("control-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			controlPort, err := cmd.Flags().GetInt("control-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			configFile, err := cmd.Flags().GetString("config")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			configBytes, err := ioutil.ReadFile(configFile)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			var config proxy.Config
			if err := yaml.Unmarshal(configBytes, &config); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			pluginsDir, err := cmd.Flags().GetString("plugins")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Create the runtime
			service := proxy.New(
				network.NewNetwork(),
				proxy.WithPluginsDir(pluginsDir),
				proxy.WithConfig(config),
				proxy.WithHost(host),
				proxy.WithPort(port),
				proxy.WithControlHost(controlHost),
				proxy.WithControlPort(controlPort),
				proxy.WithTypes(
					counterv1.Type,
					countermapv1.Type,
					indexedmapv1.Type,
					electionv1.Type,
					lockv1.Type,
					listv1.Type,
					mapv1.Type,
					multimapv1.Type,
					setv1.Type,
					topicv1.Type,
					valuev1.Type))

			// Start the runtime
			if err := service.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the runtime
			if err := service.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("config", "c", "", "the path to the router configuration")
	cmd.Flags().String("host", "", "the host to which to bind the runtime server")
	cmd.Flags().Int("port", 5678, "the port to which to bind the runtime server")
	cmd.Flags().String("control-host", "", "the host to which to bind the proxy server")
	cmd.Flags().Int("control-port", 5679, "the port to which to bind the proxy server")
	cmd.Flags().StringP("plugins", "p", "/var/atomix/plugins", "the path to the plugins directory")

	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")
	_ = cmd.MarkFlagDirname("drivers")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
