// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/pkg/controller"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/sdk/pkg/atom"
	"github.com/atomix/sdk/pkg/driver"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path/filepath"
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
			headless, err := cmd.Flags().GetBool("headless")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			ctrlHost, err := cmd.Flags().GetString("controller-host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			ctrlPort, err := cmd.Flags().GetInt("controller-port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			pluginsPath, err := cmd.Flags().GetString("plugins-path")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Connect the controller client
			ctrl, err := controller.NewClient(
				controller.WithHost(ctrlHost),
				controller.WithPort(ctrlPort))
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			opts := []runtime.Option{
				runtime.WithHost(host),
				runtime.WithPort(port),
			}
			atoms, err := ctrl.GetAtoms(context.Background())
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			for _, atom := range atoms {
				opts = append(opts, runtime.WithAtom(atom.Name, atom.Version))
			}

			rt := runtime.New(ctrl,
				atom.NewRepository(
					atom.WithPath(filepath.Join(pluginsPath)),
					atom.WithDownloader(ctrl.GetAtom)),
				driver.NewRepository(
					driver.WithPath(filepath.Join(pluginsPath)),
					driver.WithDownloader(ctrl.GetDriver)),
				runtime.WithHost(host),
				runtime.WithPort(port))
			if err := rt.Run(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the proxy service
			if err := rt.Shutdown(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("host", "h", "", "the host to which to bind the controller server")
	cmd.Flags().IntP("port", "p", 5678, "the port to which to bind the controller server")
	cmd.Flags().Bool("headless", false, "run the proxy in headless mode (without a controller)")
	cmd.Flags().String("controller-host", "atomix-controller.kube-system", "the controller host")
	cmd.Flags().Int("controller-port", 5678, "the controller port")
	cmd.Flags().String("plugins-path", ".", "the path on the file system at which to store plugins")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
