// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package version

var version string
var commit string

func Version() string {
	return version
}

func Commit() string {
	return commit
}
