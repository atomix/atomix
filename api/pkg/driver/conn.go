// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
)

type Conn interface {
	Closer
}

type Configurator interface {
	Configure(ctx context.Context, spec ConnSpec) error
}

type Closer interface {
	Close(ctx context.Context) error
}

type StoreID struct {
	Namespace string
	Name      string
}

type ConnSpec struct {
	StoreID
	Config []byte
}

func (s ConnSpec) UnmarshalConfig(config any) error {
	if s.Config == nil {
		return nil
	}
	return json.Unmarshal(s.Config, config)
}
