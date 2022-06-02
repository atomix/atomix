// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (a *Application) GetID() *ApplicationId {
	return &a.ID
}

func (a *Application) SetID(id *ApplicationId) {
	a.ID = *id
}

func (a *Application) GetVersion() ObjectVersion {
	return a.Version
}

func (a *Application) SetVersion(version ObjectVersion) {
	a.Version = version
}

var _ Object[*ApplicationId] = (*Application)(nil)
