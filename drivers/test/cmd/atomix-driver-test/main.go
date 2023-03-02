// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	apiruntimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/drivers/test/pkg/tests"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	runtimev1 "github.com/atomix/atomix/runtime/pkg/runtime/v1"
	"github.com/spf13/cobra"
	"os"
	"plugin"
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
		Use: "atomix-driver-test",
		RunE: func(cmd *cobra.Command, args []string) error {
			driverName, err := cmd.Flags().GetString("driver")
			if err != nil {
				return err
			}
			driverAPIVersion, err := cmd.Flags().GetString("api-version")
			if err != nil {
				return err
			}
			pluginPath, err := cmd.Flags().GetString("plugin")
			if err != nil {
				return err
			}
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				return err
			}

			plugin, err := plugin.Open(pluginPath)
			if err != nil {
				return err
			}
			symbol, err := plugin.Lookup("Plugin")
			if err != nil {
				return err
			}
			driver := *symbol.(*driver.Driver)

			// Initialize the runtime
			driverID := apiruntimev1.DriverID{
				Name:       driverName,
				APIVersion: driverAPIVersion,
			}
			rt := runtimev1.New(runtimev1.WithDriver(driverID, driver))

			// Start the runtime service
			svc := runtime.NewService(rt)
			if err := svc.Start(); err != nil {
				return err
			}

			tests := []testing.InternalTest{
				{
					Name: "Counter",
					F: func(t *testing.T) {
						tests.Run(t, tests.NewCounterTests(rt), timeout)
					},
				},
				{
					Name: "Map",
					F: func(t *testing.T) {
						tests.Run(t, tests.NewMapTests(rt), timeout)
					},
				},
			}

			// Hack to enable verbose testing.
			os.Args = []string{
				os.Args[0],
				"-test.v",
			}

			testing.Main(func(_, _ string) (bool, error) { return true, nil }, tests, nil, nil)
			return nil
		},
	}

	cmd.Flags().StringP("driver", "d", "", "the driver name")
	cmd.Flags().StringP("api-version", "v", "", "the driver API version")
	cmd.Flags().StringP("plugin", "p", "/var/atomix/driver.so", "the path to the driver plugin")
	cmd.Flags().DurationP("timeout", "t", 10*time.Minute, "the test timeout")

	_ = cmd.MarkFlagRequired("driver")
	_ = cmd.MarkFlagRequired("api-version")
	return cmd
}
