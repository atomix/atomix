// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (p *Primitive) GetID() *PrimitiveId {
	return &p.ID
}

func (p *Primitive) SetID(id *PrimitiveId) {
	p.ID = *id
}

func (p *Primitive) GetVersion() ObjectVersion {
	return p.Version
}

func (p *Primitive) SetVersion(version ObjectVersion) {
	p.Version = version
}

var _ Object[*PrimitiveId] = (*Primitive)(nil)
