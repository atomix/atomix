// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (b *Binding) GetMeta() ObjectMeta {
	return b.ObjectMeta
}

func (b *Binding) SetMeta(meta ObjectMeta) {
	b.ObjectMeta = meta
}

var _ Object = (*Binding)(nil)
