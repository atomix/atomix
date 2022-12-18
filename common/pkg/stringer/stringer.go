// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package stringer

import "fmt"

func Truncate(stringer fmt.Stringer, length int) fmt.Stringer {
	return &truncateStringer{
		stringer: stringer,
		length:   length,
	}
}

type truncateStringer struct {
	stringer fmt.Stringer
	length   int
}

func (s *truncateStringer) String() string {
	value := s.stringer.String()
	if len(value) > s.length {
		return value[:s.length]
	}
	return value
}
