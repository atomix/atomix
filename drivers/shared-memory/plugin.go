// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package shared_memory

import (
	"github.com/atomix/atomix/drivers/shared-memory/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
)

var Plugin = driver.New(network.NewDefaultDriver())
