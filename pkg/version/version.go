// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package version

var (
	version     string
	commit      string
	shortCommit string
	buildType   string
)

const (
	snapshot = "snapshot"
	release  = "release"
)

func Version() string {
	return version
}

func Commit() string {
	return commit
}

func ShortCommit() string {
	return shortCommit
}

func IsSnapshot() bool {
	return buildType == snapshot
}

func IsRelease() bool {
	return buildType == release
}
