// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/atomix/atomix/drivers/shared-memory/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
)

var Plugin = driver.New(network.NewDefaultDriver())
