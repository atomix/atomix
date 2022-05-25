// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (p *Proxy) GetID() *ProxyId {
	return &p.ID
}

func (p *Proxy) SetID(id *ProxyId) {
	p.ID = *id
}

func (p *Proxy) GetVersion() ObjectVersion {
	return p.Version
}

func (p *Proxy) SetVersion(version ObjectVersion) {
	p.Version = version
}

var _ Object[*ProxyId] = (*Proxy)(nil)
