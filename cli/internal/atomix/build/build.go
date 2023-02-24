// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"fmt"
	"github.com/atomix/atomix/cli/internal/exec"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"runtime"
)

var log = logging.GetLogger("github.com/atomix/atomix/cli")

const atomixCCEnabledEnv = "ATOMIX_CC_ENABLED"

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "build",
		Args: cobra.ExactArgs(1),
		Run:  runBuildExecutableCommand,
	}
	cmd.Flags().String("os", os.Getenv("GOOS"), "the operating system for which to build the executable")
	cmd.Flags().String("arch", os.Getenv("GOARCH"), "the architecture for which to build the executable")
	cmd.Flags().StringP("output", "o", "", "the path to which to write the executable")
	_ = cmd.MarkFlagRequired("output")
	cmd.AddCommand(getBuildPluginCommand())
	return cmd
}

func runBuildExecutableCommand(cmd *cobra.Command, args []string) {
	if os.Getenv(atomixCCEnabledEnv) != "" {
		runNativeBuildExecutableCommand(cmd, args)
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
			runDockerBuildExecutableCommand(cmd, args)
		} else {
			runNativeBuildExecutableCommand(cmd, args)
		}
	}
}

func runNativeBuildExecutableCommand(cmd *cobra.Command, args []string) {
	inputPath := args[0]
	outputPath, err := cmd.Flags().GetString("output")
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

	log.Infof("Building %s", inputPath)
	err = exec.Command("go", "build").
		Env(env...).
		Run("-mod=readonly",
			"-trimpath",
			"-o", outputPath,
			inputPath)
	if err != nil {
		log.Fatal(err)
	}
}

func runDockerBuildExecutableCommand(cmd *cobra.Command, args []string) {
	dockerArgs := []string{"run", "-i"}
	atomixArgs := []string{"build"}

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

	// Mount the source to the build container
	mainPath := args[0]
	if modDir, _, local, err := getLocalModFromMain(mainPath); err != nil {
		log.Fatal(err)
	} else if local {
		dockerArgs = append(dockerArgs, "--volume", modDir+":/build/source")
		dockerArgs = append(dockerArgs, "--workdir", "/build/source")
	}
	atomixArgs = append(atomixArgs, mainPath)

	// Run the Docker command
	dockerArgs = append(dockerArgs, "atomix/cli")
	if err := exec.Command("docker", dockerArgs...).Run(atomixArgs...); err != nil {
		log.Fatal(err)
	}
}
