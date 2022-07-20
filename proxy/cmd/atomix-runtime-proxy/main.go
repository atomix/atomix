// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/logging"
	counterv1 "github.com/atomix/runtime/sdk/primitives/counter/v1"
	electionv1 "github.com/atomix/runtime/sdk/primitives/election/v1"
	indexedmapv1 "github.com/atomix/runtime/sdk/primitives/indexed_map/v1"
	listv1 "github.com/atomix/runtime/sdk/primitives/list/v1"
	lockv1 "github.com/atomix/runtime/sdk/primitives/lock/v1"
	mapv1 "github.com/atomix/runtime/sdk/primitives/map/v1"
	setv1 "github.com/atomix/runtime/sdk/primitives/set/v1"
	topicv1 "github.com/atomix/runtime/sdk/primitives/topic/v1"
	valuev1 "github.com/atomix/runtime/sdk/primitives/value/v1"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

var log = logging.GetLogger()

func init() {
	logging.SetLevel(logging.DebugLevel)
}

func main() {
	cmd := &cobra.Command{
		Use: "atomix-runtime-proxy",
		Run: func(cmd *cobra.Command, args []string) {
			runtimeHost, err := cmd.Flags().GetString("runtime-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			runtimePort, err := cmd.Flags().GetInt("runtime-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			proxyHost, err := cmd.Flags().GetString("proxy-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			proxyPort, err := cmd.Flags().GetInt("proxy-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			routerConfigFile, err := cmd.Flags().GetString("config")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			routerConfigBytes, err := ioutil.ReadFile(routerConfigFile)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			var routerConfig proxy.RouterConfig
			if err := yaml.Unmarshal(routerConfigBytes, &routerConfig); err != nil {
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
				proxy.NewNetwork(),
				proxy.WithPluginsDir(pluginsDir),
				proxy.WithRouterConfig(routerConfig),
				proxy.WithRuntimeHost(runtimeHost),
				proxy.WithRuntimePort(runtimePort),
				proxy.WithProxyHost(proxyHost),
				proxy.WithProxyPort(proxyPort),
				proxy.WithTypes(
					counterv1.Type,
					electionv1.Type,
					indexedmapv1.Type,
					listv1.Type,
					lockv1.Type,
					mapv1.Type,
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
	cmd.Flags().String("runtime-host", "", "the host to which to bind the runtime server")
	cmd.Flags().Int("runtime-port", 5678, "the port to which to bind the runtime server")
	cmd.Flags().String("proxy-host", "", "the host to which to bind the proxy server")
	cmd.Flags().Int("proxy-port", 5679, "the port to which to bind the proxy server")
	cmd.Flags().StringP("plugins", "p", "/var/atomix/plugins", "the path to the plugins directory")

	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")
	_ = cmd.MarkFlagDirname("drivers")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
