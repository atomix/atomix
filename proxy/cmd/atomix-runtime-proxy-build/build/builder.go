// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"encoding/json"
	"fmt"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/rogpeppe/go-internal/modfile"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"os/exec"
)

const sdkPath = "github.com/atomix/runtime/sdk"

func Build(cmd *cobra.Command, config Config) error {
	proxyModBytes, err := ioutil.ReadFile("go.mod")
	if err != nil {
		return err
	}

	proxyModFile, err := modfile.Parse("go.mod", proxyModBytes, nil)
	if err != nil {
		return err
	}

	var sdkVersion string
	for _, require := range proxyModFile.Require {
		if require.Mod.Path == sdkPath {
			sdkVersion = require.Mod.Version
			break
		}
	}

	if err := os.MkdirAll("/build/dist/bin", os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll("/build/dist/plugins", os.ModePerm); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), "Building github.com/atomix/runtime/proxy")
	_, err = run(".",
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-gcflags=all=-N -l",
		fmt.Sprintf("-ldflags=-s -w -X github.com/atomix/runtime/sdk/pkg/version.version=%s", sdkVersion),
		"-o", "/build/dist/bin/atomix-runtime-proxy",
		"./cmd/atomix-runtime-proxy")
	if err != nil {
		return err
	}

	builder := newPluginBuilder(cmd, proxyModFile, sdkVersion)
	for _, plugin := range config.Plugins {
		if err := builder.Build(plugin); err != nil {
			return err
		}
	}
	return nil
}

func newPluginBuilder(cmd *cobra.Command, proxyModFile *modfile.File, sdkVersion string) *Builder {
	return &Builder{
		cmd:          cmd,
		sdkVersion:   sdkVersion,
		proxyModFile: proxyModFile,
	}
}

type Builder struct {
	cmd          *cobra.Command
	sdkVersion   string
	proxyModFile *modfile.File
}

func (b *Builder) Build(plugin PluginConfig) error {
	pluginModFile, pluginModDir, err := b.downloadPluginMod(plugin)
	if err != nil {
		return err
	}
	if err := b.validatePluginModFile(plugin, pluginModFile); err != nil {
		return err
	}
	if err := b.buildPlugin(plugin, pluginModDir); err != nil {
		return err
	}
	return nil
}

func (b *Builder) downloadPluginMod(plugin PluginConfig) (*modfile.File, string, error) {
	if plugin.Path == "" {
		err := errors.NewInvalid("no plugin module path configured")
		fmt.Fprintln(b.cmd.OutOrStderr(), "Plugin configuration is invalid", err)
		return nil, "", err
	}

	fmt.Fprintln(b.cmd.OutOrStdout(), "Downloading module", plugin.Path)
	output, err := run(".", "go", "mod", "download", "-json", plugin.Path)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", plugin.Path, err)
		return nil, "", err
	}
	println(output)

	var modInfo goModInfo
	if err := json.Unmarshal([]byte(output), &modInfo); err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", plugin.Path, err)
		return nil, "", err
	}

	fmt.Fprintln(b.cmd.OutOrStdout(), "Parsing", modInfo.GoMod)
	goModBytes, err := ioutil.ReadFile(modInfo.GoMod)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", plugin.Path, err)
		return nil, "", err
	}

	goModFile, err := modfile.Parse(modInfo.GoMod, goModBytes, nil)
	if err != nil {
		fmt.Fprintln(b.cmd.OutOrStderr(), "Failed to download module", plugin.Path, err)
		return nil, "", err
	}
	return goModFile, modInfo.Dir, nil
}

func (b *Builder) validatePluginModFile(plugin PluginConfig, pluginModFile *modfile.File) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Validating dependencies for module", plugin.Path)
	proxyModRequires := make(map[string]string)
	for _, require := range b.proxyModFile.Require {
		proxyModRequires[require.Mod.Path] = require.Mod.Version
	}

	for _, require := range pluginModFile.Require {
		if proxyVersion, ok := proxyModRequires[require.Mod.Path]; ok {
			if require.Mod.Version != proxyVersion {
				fmt.Fprintln(b.cmd.OutOrStderr(), "Incompatible dependency", require.Mod.Path, require.Mod.Version)
				return errors.NewInvalid("plugin module %s has incompatible dependency %s %s != %s",
					plugin.Path, require.Mod.Path, require.Mod.Version, proxyVersion)
			}
		}
	}
	return nil
}

func (b *Builder) buildPlugin(plugin PluginConfig, dir string) error {
	fmt.Fprintln(b.cmd.OutOrStdout(), "Building plugin", plugin.Name)
	_, err := run(dir,
		"go", "build",
		"-mod=readonly",
		"-trimpath",
		"-buildmode=plugin",
		"-gcflags=all=-N -l",
		fmt.Sprintf("-ldflags=-s -w -X github.com/atomix/runtime/sdk/pkg/version.version=%s", b.sdkVersion),
		"-o", fmt.Sprintf("/build/dist/plugins/%s@%s.so", plugin.Name, plugin.Version),
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
