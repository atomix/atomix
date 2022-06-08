// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
)

type Client interface {
	Closer
}

type Configurator[C any] interface {
	Configure(ctx context.Context, config C) error
}

type Creator interface {
	Create(ctx context.Context) error
}

type Closer interface {
	Close(ctx context.Context) error
}
