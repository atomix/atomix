// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/runtime/proxy/cmd/atomix-runtime-proxy-build/build"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
)

func init() {
	logging.SetLevel(logging.DebugLevel)
}

func main() {
	cmd := &cobra.Command{
		Use: "atomix-runtime-proxy-build",
		Run: func(cmd *cobra.Command, args []string) {
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

			var config build.Config
			if err := yaml.Unmarshal(configBytes, &config); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			if err := build.Build(cmd, config); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("config", "c", "", "the path to the build configuration")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
