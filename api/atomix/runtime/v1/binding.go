// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (b *Binding) GetID() *BindingId {
	return &b.ID
}

func (b *Binding) SetID(id *BindingId) {
	b.ID = *id
}

func (b *Binding) GetVersion() ObjectVersion {
	return b.Version
}

func (b *Binding) SetVersion(version ObjectVersion) {
	b.Version = version
}

var _ Object[*BindingId] = (*Binding)(nil)
