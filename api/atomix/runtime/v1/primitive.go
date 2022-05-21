// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (p *Primitive) GetMeta() ObjectMeta {
	return p.ObjectMeta
}

func (p *Primitive) SetMeta(meta ObjectMeta) {
	p.ObjectMeta = meta
}

var _ Object = (*Primitive)(nil)
