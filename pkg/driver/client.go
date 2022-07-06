// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/runtime/pkg/runtime"
)

type Client interface {
	runtime.Client
	Closer
}

type Configurator[C any] interface {
	Configure(ctx context.Context, config C) error
}

type Closer interface {
	Close(ctx context.Context) error
}
