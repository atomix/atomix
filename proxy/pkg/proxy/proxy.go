// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import "github.com/atomix/runtime/sdk/pkg/service"

func New(network Network, opts ...Option) *Proxy {
	var options Options
	options.apply(opts...)
	runtime := newRuntime(options)
	return &Proxy{
		Options: options,
		runtime: newRuntimeService(runtime, network, options.Config.Server, options.RuntimeService),
		service: newProxyService(runtime, network, options.ProxyService),
	}
}

type Proxy struct {
	Options
	runtime service.Service
	service service.Service
}

func (p *Proxy) Start() error {
	if err := p.service.Start(); err != nil {
		return err
	}
	if err := p.runtime.Start(); err != nil {
		return err
	}
	return nil
}

func (p *Proxy) Stop() error {
	if err := p.runtime.Stop(); err != nil {
		return err
	}
	if err := p.service.Stop(); err != nil {
		return err
	}
	return nil
}

var _ service.Service = (*Proxy)(nil)
