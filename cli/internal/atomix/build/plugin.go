// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"fmt"
	"github.com/atomix/atomix/cli/internal/exec"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"runtime"
)

func getBuildPluginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "plugin",
		Aliases: []string{"driver"},
		Args:    cobra.ExactArgs(1),
		Run:     runBuildPluginCommand,
	}
	cmd.Flags().String("os", os.Getenv("GOOS"), "the operating system for which to build the plugin")
	cmd.Flags().String("arch", os.Getenv("GOARCH"), "the architecture for which to build the plugin")
	cmd.Flags().StringP("target", "t", ".", "the path to the target module")
	cmd.Flags().StringP("output", "o", "", "the path to which to write the plugin")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func runBuildPluginCommand(cmd *cobra.Command, args []string) {
	if os.Getenv(atomixCCEnabledEnv) != "" {
		runNativeBuildPluginCommand(cmd, args)
	} else {
		goOS, err := cmd.Flags().GetString("os")
		if err != nil {
			log.Fatal(err)
		}
		goArch, err := cmd.Flags().GetString("arch")
		if err != nil {
			log.Fatal(err)
		}
		if (goOS != "" && goOS != runtime.GOOS) || (goArch != "" && goArch != runtime.GOARCH) {
			runDockerBuildPluginCommand(cmd, args)
		} else {
			runNativeBuildPluginCommand(cmd, args)
		}
	}
}

func runNativeBuildPluginCommand(cmd *cobra.Command, args []string) {
	inputPath := args[0]
	targetPath, err := cmd.Flags().GetString("target")
	if err != nil {
		log.Fatal(err)
	}
	outputPath, err := cmd.Flags().GetString("output")
	if err != nil {
		log.Fatal(err)
	}
	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		log.Fatal(err)
	}

	env := []string{
		"GO111MODULE=on",
		"CGO_ENABLED=1",
	}

	goOS, err := cmd.Flags().GetString("os")
	if err != nil {
		log.Fatal(err)
	} else if goOS != "" {
		env = append(env, fmt.Sprintf("GOOS=%s", goOS))
	}

	goArch, err := cmd.Flags().GetString("arch")
	if err != nil {
		log.Fatal(err)
	} else if goArch != "" {
		env = append(env, fmt.Sprintf("GOARCH=%s", goArch))
	}

	// Split the source path@version pair
	sourcePath, _, err := splitPathVersion(inputPath)
	if err != nil {
		log.Fatal(err)
	}

	// Fetch the source (driver) module
	log.Infof("Fetching source module %s", inputPath)
	sourceModDir, sourceModFile, err := fetchModFromMain(inputPath)
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
		Env(env...).
		Dir(tempModDir).
		Run("-mod=readonly",
			"-trimpath",
			"-buildmode=plugin",
			"-o", outputPath,
			sourcePath)
	if err != nil {
		log.Fatal(err)
	}
}

func runDockerBuildPluginCommand(cmd *cobra.Command, args []string) {
	dockerArgs := []string{"run", "-i"}
	atomixArgs := []string{"build", "plugin"}

	// Pass the log level through to the atomix-build command
	logLevel, err := cmd.Flags().GetString("log-level")
	if err != nil {
		log.Fatal(err)
	} else if logLevel != "" {
		atomixArgs = append(atomixArgs, "--log-level", logLevel)
	}

	// Add the OS to the build arguments
	goOS, err := cmd.Flags().GetString("os")
	if err != nil {
		log.Fatal(err)
	} else if goOS != "" {
		atomixArgs = append(atomixArgs, "--os", goOS)
	} else {
		atomixArgs = append(atomixArgs, "--os", runtime.GOOS)
	}

	// Add the architecture to the build arguments
	goArch, err := cmd.Flags().GetString("arch")
	if err != nil {
		log.Fatal(err)
	} else if goArch != "" {
		atomixArgs = append(atomixArgs, "--arch", goArch)
	} else {
		atomixArgs = append(atomixArgs, "--arch", runtime.GOARCH)
	}

	// Mount the plugin source to the build container
	mainPath := args[0]
	if modDir, _, local, err := getLocalModFromMain(mainPath); err != nil {
		log.Fatal(err)
	} else if local {
		dockerArgs = append(dockerArgs, "--volume", modDir+":/build/source")
		dockerArgs = append(dockerArgs, "--workdir", "/build/source")
	}
	atomixArgs = append(atomixArgs, mainPath)

	// Create and validate the output directory
	outputPath, err := cmd.Flags().GetString("output")
	if err != nil {
		log.Fatal(err)
	}
	if fileInfo, err := os.Stat(outputPath); err != nil {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(outputPath), os.ModeDir); err != nil {
			log.Fatal(err)
		}
	} else if fileInfo.IsDir() {
		log.Fatalf("--output/-o cannot be a directory")
	}

	// Mount the output directory to the build container
	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		log.Fatal(err)
	}
	dockerArgs = append(dockerArgs, "--volume", filepath.Dir(outputPath)+":/build/output")
	atomixArgs = append(atomixArgs, "--output", filepath.Join("/build/output", filepath.Base(outputPath)))

	// Mount the target path to the build container if specified
	targetPath, err := cmd.Flags().GetString("target")
	if err != nil {
		log.Fatal(err)
	}
	if modPath, _, ok, err := getLocalMod(targetPath); err != nil {
		log.Fatal(err)
	} else if ok {
		dockerArgs = append(dockerArgs, "--volume", modPath+":/build/target")
		atomixArgs = append(atomixArgs, "--target", "/build/target")
	} else {
		atomixArgs = append(atomixArgs, "--target", targetPath)
	}

	// Run the Docker command
	dockerArgs = append(dockerArgs, "atomix/build")
	if err := exec.Command("docker", dockerArgs...).Run(atomixArgs...); err != nil {
		log.Fatal(err)
	}
}
