// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/atomix/test/internal/tests"
	"github.com/spf13/cobra"
	"testing"
	"time"
)

func main() {
	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "atomix-test",
		Args: cobra.ExactArgs(1),
		RunE: runCommand,
	}
	cmd.Flags().StringP("kind", "k", "", "the primitive kind to test")
	_ = cmd.MarkFlagRequired("kind")
	cmd.Flags().StringP("api-version", "v", "v1", "the primitive API version to test")
	cmd.Flags().DurationP("timeout", "t", 10*time.Minute, "the test timeout")
	return cmd
}

func runCommand(cmd *cobra.Command, args []string) error {
	name := args[0]
	kind, err := cmd.Flags().GetString("kind")
	if err != nil {
		return err
	}
	apiVersion, err := cmd.Flags().GetString("api-version")
	if err != nil {
		return err
	}
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return err
	}

	switch kind {
	case "Counter":
		switch apiVersion {
		case "v1":
			runTest("CounterV1", tests.GetCounterV1Test(name, timeout))
		default:
			return fmt.Errorf("unsupported test API version %s", apiVersion)
		}
	case "Map":
		switch apiVersion {
		case "v1":
			runTest("MapV1", tests.GetMapV1Test(name, timeout))
		default:
			return fmt.Errorf("unsupported test API version %s", apiVersion)
		}
	default:
		return fmt.Errorf("unsupported test kind %s", kind)
	}
	return nil
}

func runTest(name string, f func(*testing.T)) {
	testing.Main(func(_, _ string) (bool, error) { return true, nil }, []testing.InternalTest{
		{
			Name: name,
			F: func(t *testing.T) {
				t.Logf("Running %s", name)
				f(t)
			},
		},
	}, nil, nil)
}
