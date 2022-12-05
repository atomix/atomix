// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/rogpeppe/go-internal/modfile"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

const sdkPath = "github.com/atomix/runtime/sdk"

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
		Use:  "driver",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputPath, outputPath := args[0], args[1]
			builder, err := newBuilder(cmd)
			if err != nil {
				return err
			}
			return builder.buildDriver(inputPath, outputPath)
		},
	})

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func newBuilder(cmd *cobra.Command) (*atomixBuilder, error) {
	proxyModBytes, err := ioutil.ReadFile("go.mod")
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
	fmt.Fprintln(b.cmd.OutOrStdout(), "Building github.com/atomix/runtime/proxy")
	_, err := run(".",
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-gcflags=all=-N -l",
		"-o", outputPath,
		"./cmd/atomix-runtime-proxy")
	return err
}

func (b *atomixBuilder) buildDriver(inputPath, outputPath string) error {
	pluginModFile, pluginModDir, err := b.downloadPluginMod(inputPath)
	if err != nil {
		return err
	}
	if err := b.validatePluginModFile(inputPath, pluginModFile); err != nil {
		return err
	}
	if err := b.buildPlugin(outputPath, pluginModDir); err != nil {
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
	goModBytes, err := ioutil.ReadFile(modInfo.GoMod)
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

func (b *atomixBuilder) buildPlugin(outputPath string, dir string) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Building plugin", filepath.Base(outputPath))
	_, err := run(dir,
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-buildmode=plugin",
		"-gcflags=all=-N -l",
		"-o", outputPath,
		"./plugin")
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
