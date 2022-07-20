// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import "os"

const namespaceEnv = "ATOMIX_NAMESPACE"

func GetNamespace() string {
	return os.Getenv(namespaceEnv)
}
