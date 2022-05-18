// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

type Config struct {
	Clusters map[string]ClusterConfig `yaml:"clusters,omitempty"`
}

type ClusterConfig struct {
	Name   string                 `yaml:"name"`
	Driver DriverConfig           `yaml:"driver,omitempty"`
	Config map[string]interface{} `yaml:"config,omitempty"`
}

type DriverConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}
