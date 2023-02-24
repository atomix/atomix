// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix_build

import (
	"fmt"
	"github.com/atomix/atomix/build/internal/exec"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var log = logging.GetLogger("github.com/atomix/atomix/build")

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "atomix-build",
		Args: cobra.ExactArgs(1),
		Run:  runBuildExecutableCommand,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logLevel, err := cmd.Flags().GetString("log-level")
			if err != nil {
				return err
			}
			switch strings.ToLower(logLevel) {
			case "debug":
				log.SetLevel(logging.DebugLevel)
			case "info":
				log.SetLevel(logging.InfoLevel)
			case "warn":
				log.SetLevel(logging.WarnLevel)
			case "error":
				log.SetLevel(logging.ErrorLevel)
			case "fatal":
				log.SetLevel(logging.FatalLevel)
			default:
				log.SetLevel(logging.InfoLevel)
			}
			return nil
		},
	}
	cmd.PersistentFlags().StringP("log-level", "l", "info", "the log level")
	cmd.Flags().String("os", os.Getenv("GOOS"), "the operating system for which to build the executable")
	cmd.Flags().String("arch", os.Getenv("GOARCH"), "the architecture for which to build the executable")
	cmd.Flags().StringP("output", "o", "", "the path to which to write the executable")
	_ = cmd.MarkFlagRequired("output")
	cmd.AddCommand(getBuildPluginCommand())
	return cmd
}

func runBuildExecutableCommand(cmd *cobra.Command, args []string) {
	inputPath := args[0]
	outputPath, err := cmd.Flags().GetString("output")
	if err != nil {
		log.Fatal(err)
	}
	goOS, err := cmd.Flags().GetString("os")
	if err != nil {
		log.Fatal(err)
	}
	goArch, err := cmd.Flags().GetString("arch")
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Building %s", inputPath)
	err = exec.Command("go", "build").
		Env("GO111MODULE=on",
			"CGO_ENABLED=1",
			"CC=gcc",
			"CXX=g++",
			fmt.Sprintf("GOOS=%s", goOS),
			fmt.Sprintf("GOARCH=%s", goArch)).
		Run("-mod=readonly",
			"-trimpath",
			"-gcflags=all=-N -l",
			"-o", outputPath,
			inputPath)
	if err != nil {
		log.Fatal(err)
	}
}
