// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
)

type Client interface {
	Closer
}

type Configurator[C any] interface {
	Configure(ctx context.Context, config C) error
}

type Closer interface {
	Close(ctx context.Context) error
}
