// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"github.com/atomix/atomix/cli/internal/atomix/build"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/spf13/cobra"
	"strings"
)

var log = logging.GetLogger("github.com/atomix/atomix/cli")

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "atomix",
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
	cmd.AddCommand(build.GetCommand())
	cmd.PersistentFlags().StringP("log-level", "l", "info", "the log level")
	return cmd
}
