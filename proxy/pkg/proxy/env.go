// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import "os"

const (
	namespaceEnv = "ATOMIX_NAMESPACE"
	profileEnv   = "ATOMIX_PROFILE"
)

func getNamespace() string {
	return os.Getenv(namespaceEnv)
}

func getProfile() string {
	return os.Getenv(profileEnv)
}
