// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

type Starter interface {
	Start() error
}

type Stopper interface {
	Stop() error
}

type Service interface {
	Starter
	Stopper
}
