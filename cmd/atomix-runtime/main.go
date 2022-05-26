// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	primitiveservice "github.com/atomix/runtime/pkg/primitive/service"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/runtime/controller"
	runtimeservice "github.com/atomix/runtime/pkg/runtime/service"
	counterv1 "github.com/atomix/runtime/primitives/counter/v1"
	electionv1 "github.com/atomix/runtime/primitives/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/indexed_map/v1"
	listv1 "github.com/atomix/runtime/primitives/list/v1"
	lockv1 "github.com/atomix/runtime/primitives/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/map/v1"
	setv1 "github.com/atomix/runtime/primitives/set/v1"
	topicv1 "github.com/atomix/runtime/primitives/topic/v1"
	valuev1 "github.com/atomix/runtime/primitives/value/v1"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-runtime",
		Run: func(cmd *cobra.Command, args []string) {
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
			cacheDir, err := cmd.Flags().GetString("cache")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Create the runtime
			runtime := runtime.New(
				runtime.WithConfigFile(configFile),
				runtime.WithCacheDir(cacheDir))

			// Start the runtime controller
			runtimeController := controller.NewController(runtime)
			if err := runtimeController.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Start the runtime service
			runtimeService := runtimeservice.NewService(runtime,
				runtimeservice.WithHost(proxyHost),
				runtimeservice.WithPort(proxyPort))
			if err := runtimeService.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Start the primitive service
			primitiveService := primitiveservice.NewService(runtime,
				primitiveservice.WithHost(controlHost),
				primitiveservice.WithPort(controlPort),
				primitiveservice.WithPrimitiveKinds(
					counterv1.Kind,
					electionv1.Kind,
					indexedmapv1.Kind,
					listv1.Kind,
					lockv1.Kind,
					mapv1.Kind,
					setv1.Kind,
					topicv1.Kind,
					valuev1.Kind))
			if err := primitiveService.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the services
			if err := primitiveService.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := runtimeService.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// Stop the controllers
			if err := runtimeController.Stop(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("proxy-host", "h", "", "the host to which to bind the proxy server")
	cmd.Flags().IntP("proxy-port", "p", 5678, "the port to which to bind the proxy server")
	cmd.Flags().String("control-host", "", "the host to which to bind the runtime server")
	cmd.Flags().Int("control-port", 5679, "the port to which to bind the runtime server")
	cmd.Flags().StringP("config", "c", "~/.atomix/config.yaml", "the path to the Atomix configuration file")
	cmd.Flags().String("cache", "~/.atomix/cache", "the path to the Atomix cache")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
