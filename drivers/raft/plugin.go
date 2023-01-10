// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"github.com/atomix/atomix/drivers/raft/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
)

var Plugin = driver.New(network.NewDefaultDriver())
