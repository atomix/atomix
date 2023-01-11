// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/drivers/pod-memory/v1/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
)

var Plugin = driver.New(network.NewLocalDriver())
