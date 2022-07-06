// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
)

func NewKind(name, apiVersion string) Kind {
	return Kind{
		Name:       name,
		APIVersion: apiVersion,
	}
}

type Kind struct {
	Name       string
	APIVersion string
}

func (k Kind) String() string {
	return fmt.Sprintf("%s/%s", k.Name, k.APIVersion)
}

type Runtime interface {
	GetClient(ctx context.Context, kind Kind, name string) (Client, error)
}

type Client interface{}
