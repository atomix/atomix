// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (d *Driver) GetID() *DriverId {
	return &d.ID
}

func (d *Driver) SetID(id *DriverId) {
	d.ID = *id
}

func (d *Driver) GetVersion() ObjectVersion {
	return d.Version
}

func (d *Driver) SetVersion(version ObjectVersion) {
	d.Version = version
}

var _ Object[*DriverId] = (*Driver)(nil)
