// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/rogpeppe/go-internal/modfile"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func init() {
	logging.SetLevel(logging.DebugLevel)
}

func main() {
	cmd := &cobra.Command{
		Use: "atomix-build",
	}
	cmd.AddCommand(&cobra.Command{
		Use:  "proxy",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputPath := args[0]
			builder, err := newBuilder(cmd)
			if err != nil {
				return err
			}
			return builder.buildProxy(outputPath)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:  "validate",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputPath := args[0]
			builder, err := newBuilder(cmd)
			if err != nil {
				return err
			}
			return builder.validateDriver(inputPath)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:  "driver",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputPath, pluginPath, outputPath := args[0], args[1], args[2]
			builder, err := newBuilder(cmd)
			if err != nil {
				return err
			}
			return builder.buildDriver(inputPath, pluginPath, outputPath)
		},
	})

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func newBuilder(cmd *cobra.Command) (*atomixBuilder, error) {
	proxyModBytes, err := os.ReadFile("go.mod")
	if err != nil {
		return nil, err
	}

	proxyModFile, err := modfile.Parse("go.mod", proxyModBytes, nil)
	if err != nil {
		return nil, err
	}

	return &atomixBuilder{
		cmd:          cmd,
		proxyModFile: proxyModFile,
	}, nil
}

type atomixBuilder struct {
	cmd          *cobra.Command
	proxyModFile *modfile.File
}

func (b *atomixBuilder) buildProxy(outputPath string) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Building github.com/atomix/atomix/proxy")
	_, err := run(".",
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-gcflags=all=-N -l",
		"-o", outputPath,
		"./cmd/atomix-proxy")
	return err
}

func (b *atomixBuilder) validateDriver(inputPath string) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Parsing", inputPath)
	goModBytes, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to validate module", inputPath, err)
		return err
	}
	goModFile, err := modfile.Parse(inputPath, goModBytes, nil)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to validate module", inputPath, err)
		return err
	}
	if err := b.validatePluginModFile(inputPath, goModFile); err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to validate module", inputPath, err)
		return err
	}
	fmt.Fprintf(b.cmd.OutOrStdout(), "Module %s is compatible with this version of the proxy!\n", inputPath)
	return nil
}

func (b *atomixBuilder) buildDriver(inputPath, pluginPath, outputPath string) error {
	inputPathParts := strings.Split(inputPath, "@")
	if len(inputPathParts) != 2 {
		return errors.NewInvalid("input path must be of the form {path}@{version}")
	}
	modPath := inputPathParts[0]
	relPath, err := filepath.Rel(modPath, pluginPath)
	if err != nil {
		return err
	}
	pluginModFile, pluginModDir, err := b.downloadPluginMod(inputPath)
	if err != nil {
		return err
	}
	if err := b.validatePluginModFile(inputPath, pluginModFile); err != nil {
		return err
	}
	if err := b.buildPlugin(filepath.Join(pluginModDir, relPath), outputPath); err != nil {
		return err
	}
	return nil
}

func (b *atomixBuilder) downloadPluginMod(inputPath string) (*modfile.File, string, error) {
	if inputPath == "" {
		err := errors.NewInvalid("no plugin module path configured")
		fmt.Fprintln(b.cmd.OutOrStderr(), "Plugin configuration is invalid", err)
		return nil, "", err
	}

	fmt.Fprintln(b.cmd.OutOrStdout(), "Downloading module", inputPath)
	output, err := run(".", "go", "mod", "download", "-json", inputPath)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", inputPath, err)
		return nil, "", err
	}
	println(output)

	var modInfo goModInfo
	if err := json.Unmarshal([]byte(output), &modInfo); err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", inputPath, err)
		return nil, "", err
	}

	fmt.Fprintln(b.cmd.OutOrStdout(), "Parsing", modInfo.GoMod)
	goModBytes, err := os.ReadFile(modInfo.GoMod)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", inputPath, err)
		return nil, "", err
	}

	goModFile, err := modfile.Parse(modInfo.GoMod, goModBytes, nil)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", inputPath, err)
		return nil, "", err
	}
	return goModFile, modInfo.Dir, nil
}

func (b *atomixBuilder) validatePluginModFile(inputPath string, pluginModFile *modfile.File) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Validating dependencies for module", inputPath)
	proxyModRequires := make(map[string]string)
	for _, require := range b.proxyModFile.Require {
		proxyModRequires[require.Mod.Path] = require.Mod.Version
	}

	for _, require := range pluginModFile.Require {
		if proxyVersion, ok := proxyModRequires[require.Mod.Path]; ok {
			if require.Mod.Version != proxyVersion {
				fmt.Fprintln(b.cmd.OutOrStderr(), "Incompatible dependency", require.Mod.Path, require.Mod.Version)
				return errors.NewInvalid("plugin module %s has incompatible dependency %s %s != %s",
					inputPath, require.Mod.Path, require.Mod.Version, proxyVersion)
			}
		}
	}
	return nil
}

func (b *atomixBuilder) buildPlugin(inputPath, outputPath string) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Building plugin", filepath.Base(outputPath))
	_, err := run(inputPath,
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-buildmode=plugin",
		"-gcflags=all=-N -l",
		"-o", outputPath,
		".")
	if err != nil {
		return err
	}
	return nil
}

func run(dir string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GO111MODULE=on",
		"CGO_ENABLED=1",
		"GOOS=linux",
		"GOARCH=amd64",
		"CC=gcc",
		"CXX=g++")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

type goModInfo struct {
	Path     string
	Version  string
	Error    string
	Info     string
	GoMod    string
	Zip      string
	Dir      string
	Sum      string
	GoModSum string
}
