// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-controller",
		Run: func(cmd *cobra.Command, args []string) {
			host, err := cmd.Flags().GetString("host")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			server := grpc.NewServer()

			// TODO: Register services

			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
			}

			go func() {
				if err := server.Serve(lis); err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}()

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			server.Stop()
		},
	}
	cmd.Flags().StringP("host", "h", "", "the host to which to bind the controller server")
	cmd.Flags().IntP("port", "p", 5678, "the port to which to bind the controller server")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
