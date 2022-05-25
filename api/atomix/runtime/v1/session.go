// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

func (s *Session) GetID() *SessionId {
	return &s.ID
}

func (s *Session) SetID(id *SessionId) {
	s.ID = *id
}

func (s *Session) GetVersion() ObjectVersion {
	return s.Version
}

func (s *Session) SetVersion(version ObjectVersion) {
	s.Version = version
}

var _ Object[*SessionId] = (*Session)(nil)
