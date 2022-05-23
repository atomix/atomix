// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/service"
)

func NewController(runtime runtime.Runtime) *Controller {
	return &Controller{
		clusters: newClusterExecutor(runtime),
		bindings: newBindingExecutor(runtime),
		proxies:  newProxyExecutor(runtime),
	}
}

type Controller struct {
	clusters Executor
	bindings Executor
	proxies  Executor
}

func (c *Controller) Start() error {
	if err := c.clusters.Start(); err != nil {
		return err
	}
	if err := c.bindings.Start(); err != nil {
		return err
	}
	if err := c.proxies.Start(); err != nil {
		return err
	}
	return nil
}

func (c *Controller) Stop() error {
	if err := c.proxies.Stop(); err != nil {
		return err
	}
	if err := c.bindings.Stop(); err != nil {
		return err
	}
	if err := c.clusters.Stop(); err != nil {
		return err
	}
	return nil
}

var _ service.Service = (*Controller)(nil)
