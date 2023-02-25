// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"github.com/atomix/atomix/test/internal/tests"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"os"
	"testing"
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
		RunE: runCommand,
	}
	cmd.Flags().StringP("suite", "s", "/etc/atomix/suite.yaml", "the path to the test configuration")
	_ = cmd.MarkFlagFilename("suite")
	return cmd
}

func runCommand(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var config Config
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return err
	}

	var goTests []testing.InternalTest
	kindCounts := make(map[string]int)
	for _, test := range config.Tests {
		name := test.Name
		if name == "" {
			kindVersion := fmt.Sprintf("%s_%s", test.Kind, test.APIVersion)
			kindCounts[kindVersion] = kindCounts[kindVersion] + 1
			name = fmt.Sprintf("%s_%d", kindVersion, kindCounts[kindVersion])
		}
		var testF func(*testing.T)
		switch test.Kind {
		case "Counter":
			switch test.APIVersion {
			case "v1":
				testF = tests.GetCounterV1Test(name)
			default:
				return fmt.Errorf("unsupported API version %s", test.APIVersion)
			}
		case "Map":
			switch test.APIVersion {
			case "v1":
				testF = tests.GetMapV1Test(name)
			default:
				return fmt.Errorf("unsupported API version %s", test.APIVersion)
			}
		}
		goTests = append(goTests, testing.InternalTest{
			Name: name,
			F:    testF,
		})
	}

	testing.Main(func(_, _ string) (bool, error) { return true, nil }, goTests, nil, nil)
	return nil
}

type Config struct {
	Tests []TestConfig `yaml:"config"`
}

type TestConfig struct {
	Kind       string         `yaml:"kind"`
	APIVersion string         `yaml:"apiVersion"`
	Name       string         `yaml:"name"`
	Config     map[string]any `yaml:"config"`
}
