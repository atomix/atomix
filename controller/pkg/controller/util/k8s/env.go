// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"fmt"
	"os"
)

const (
	nameEnv      = "CONTROLLER_NAME"
	namespaceEnv = "CONTROLLER_NAMESPACE"
)

const (
	defaultNamespace = "kube-system"
)

// GetName :
func GetName() string {
	name := os.Getenv(nameEnv)
	if name == "" {
		panic(fmt.Sprintf("'%s' environment variable not defined", nameEnv))
	}
	return name
}

// GetNamespace :
func GetNamespace() string {
	namespace := os.Getenv(namespaceEnv)
	if namespace != "" {
		return namespace
	}
	return defaultNamespace
}
