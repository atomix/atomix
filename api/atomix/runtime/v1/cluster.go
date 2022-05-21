// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (c *Cluster) GetMeta() ObjectMeta {
	return c.ObjectMeta
}

func (c *Cluster) SetMeta(meta ObjectMeta) {
	c.ObjectMeta = meta
}

var _ Object = (*Cluster)(nil)
