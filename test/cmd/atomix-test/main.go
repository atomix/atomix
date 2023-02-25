// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/test/internal/tests"
	"github.com/spf13/cobra"
	"testing"
)

var log = logging.GetLogger()

func main() {
	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "atomix-test",
		Run: runCommand,
	}
	return cmd
}

func runCommand(cmd *cobra.Command, args []string) {
	tests := []testing.InternalTest{
		{
			Name: "TestCounter",
			F:    tests.TestCounter,
		},
		{
			Name: "TestMap",
			F:    tests.TestMap,
		},
	}
	testing.Main(func(_, _ string) (bool, error) { return true, nil }, tests, nil, nil)
}
