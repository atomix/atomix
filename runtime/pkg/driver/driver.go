// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
)

// Driver is the primary interface for implementing storage drivers
type Driver interface{}

// Conn is a connection to a store
// Implement the Configurator interface to support configuration changes to an existing connection
type Conn interface {
	Closer
}

// Connector is an interface for connecting to a store
type Connector[T any] interface {
	Connect(ctx context.Context, spec T) (Conn, error)
}

// Configurator is an interface for supporting configuration changes on an existing Conn
type Configurator[T any] interface {
	Configure(ctx context.Context, spec T) error
}

// Closer is an interface for closing connections
type Closer interface {
	Close(ctx context.Context) error
}
