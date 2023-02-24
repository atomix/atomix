// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix_build

import (
	"fmt"
	"github.com/atomix/atomix/build/internal/exec"
	"github.com/spf13/cobra"
	"os"
)

func getBuildPluginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "plugin",
		Aliases: []string{"driver"},
		Args:    cobra.ExactArgs(1),
		Run:     runBuildPluginCommand,
	}
	cmd.Flags().String("os", os.Getenv("GOOS"), "the operating system for which to build the executable")
	cmd.Flags().String("arch", os.Getenv("GOARCH"), "the architecture for which to build the executable")
	cmd.Flags().StringP("target", "t", ".", "the path to the target module")
	cmd.Flags().StringP("output", "o", "", "the path to which to write the driver plugin")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func runBuildPluginCommand(cmd *cobra.Command, args []string) {
	inputPath := args[0]
	targetPath, err := cmd.Flags().GetString("target")
	if err != nil {
		log.Fatal(err)
	}
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

	// Split the source path@version pair
	sourcePath, _, err := splitPathVersion(inputPath)
	if err != nil {
		log.Fatal(err)
	}

	// Fetch the source (driver) module
	log.Infof("Fetching source module %s", inputPath)
	sourceModDir, sourceModFile, err := fetchMod(inputPath)
	if err != nil {
		log.Fatal(err)
	}

	// Create a temporary directory for modifying the source module
	tempModDir, err := os.MkdirTemp("", "atomix-driver")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempModDir)

	// Copy the source module to the temporary directory
	if err := copyDir(sourceModDir, tempModDir); err != nil {
		log.Fatal(err)
	}

	// Fetch the target (runtime) module
	log.Infof("Fetching target module %s", targetPath)
	_, targetModFile, err := fetchMod(targetPath)
	if err != nil {
		log.Fatal(err)
	}

	// Map out the target module dependency versions
	targetModDeps := make(map[string]string)
	for _, targetModRequire := range targetModFile.Require {
		targetModDeps[targetModRequire.Mod.Path] = targetModRequire.Mod.Version
	}

	// Iterate through the source module dependencies and update them to match the target module dependencies if necessary
	for _, sourceModRequire := range sourceModFile.Require {
		if targetModDepVersion, ok := targetModDeps[sourceModRequire.Mod.Path]; ok && sourceModRequire.Mod.Version != targetModDepVersion {
			log.Infof("Detected dependency conflict for %s; updating to %s", sourceModRequire.Mod.Path, targetModDepVersion)
			err := exec.Command("go", "get", joinPathVersion(sourceModRequire.Mod.Path, targetModDepVersion)).
				Dir(tempModDir).
				Run()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// Build the source module from the temporary module directory
	log.Infof("Building %s", sourcePath)
	err = exec.Command("go", "build").
		Env("GO111MODULE=on",
			"CGO_ENABLED=1",
			"CC=gcc",
			"CXX=g++",
			fmt.Sprintf("GOOS=%s", goOS),
			fmt.Sprintf("GOARCH=%s", goArch)).
		Dir(tempModDir).
		Run("-mod=readonly",
			"-trimpath",
			"-buildmode=plugin",
			"-gcflags=all=-N -l",
			"-o", outputPath,
			sourcePath)
	if err != nil {
		log.Fatal(err)
	}
}
