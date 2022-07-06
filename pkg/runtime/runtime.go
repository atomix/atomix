// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
)

func NewID(namespace, name string) ID {
	return ID{
		Namespace: namespace,
		Name:      name,
	}
}

type ID struct {
	Namespace string
	Name      string
}

func (i ID) String() string {
	if i.Namespace == "" {
		return i.Name
	}
	return fmt.Sprintf("%s.%s", i.Namespace, i.Name)
}

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
	Namespace() string
	GetClient(ctx context.Context, kind Kind, id ID) (Client, error)
}

type Client interface{}
