// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package env

import "os"

const (
	namespaceEnv     = "NAMESPACE"
	applicationIDEnv = "APPLICATION_ID"
	nodeIDEnv        = "NODE_ID"
)

func GetNamespace() string {
	return os.Getenv(namespaceEnv)
}

func GetApplicationID() string {
	return os.Getenv(applicationIDEnv)
}

func GetNodeID() string {
	return os.Getenv(nodeIDEnv)
}
