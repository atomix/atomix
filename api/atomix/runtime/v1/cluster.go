// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (c *Cluster) GetID() *ClusterId {
	return &c.ID
}

func (c *Cluster) SetID(id *ClusterId) {
	c.ID = *id
}

func (c *Cluster) GetVersion() ObjectVersion {
	return c.Version
}

func (c *Cluster) SetVersion(version ObjectVersion) {
	c.Version = version
}

var _ Object[*ClusterId] = (*Cluster)(nil)
