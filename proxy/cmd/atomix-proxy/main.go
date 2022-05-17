// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/runtime/proxy/pkg/runtime"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-proxy",
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

			rt := runtime.New(
				runtime.WithProxyHost(proxyHost),
				runtime.WithProxyPort(proxyPort),
				runtime.WithRuntimeHost(runtimeHost),
				runtime.WithRuntimePort(runtimePort),
				runtime.WithConfigFile(configFile),
				runtime.WithCacheDir(cacheDir))
			if err := rt.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the proxy service
			if err := rt.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("proxy-host", "h", "", "the host to which to bind the proxy server")
	cmd.Flags().IntP("proxy-port", "p", 5678, "the port to which to bind the proxy server")
	cmd.Flags().String("runtime-host", "", "the host to which to bind the runtime server")
	cmd.Flags().Int("runtime-port", 5679, "the port to which to bind the runtime server")
	cmd.Flags().StringP("config", "c", "~/.atomix/config.yaml", "the path to the Atomix configuration file")
	cmd.Flags().String("cache", "~/.atomix/cache", "the path to the Atomix cache")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
