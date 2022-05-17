// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/rogpeppe/go-internal/modfile"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path/filepath"
)

const runtimePath = "github.com/atomix/runtime"

func main() {
	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "version",
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			file, err := cmd.Flags().GetString("file")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err)
				os.Exit(1)
			}

			var path string
			if len(args) == 1 {
				path = filepath.Join(args[0], file)
			} else {
				dir, err := os.Getwd()
				if err != nil {
					fmt.Fprintln(cmd.OutOrStderr(), err)
					os.Exit(1)
				}
				path = filepath.Join(dir, file)
			}

			bytes, err := ioutil.ReadFile(path)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err)
				os.Exit(1)
			}

			mod, err := modfile.Parse(filepath.Base(file), bytes, nil)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err)
				os.Exit(1)
			}

			for _, replace := range mod.Replace {
				if replace.Old.Path == runtimePath {
					cmd.OutOrStdout().Write([]byte(replace.New.Version))
					os.Exit(0)
				}
			}

			for _, require := range mod.Require {
				if require.Mod.Path == runtimePath {
					cmd.OutOrStdout().Write([]byte(require.Mod.Version))
					os.Exit(0)
				}
			}
		},
	}
	cmd.Flags().StringP("file", "f", "go.mod", "the module file")
	return cmd
}
