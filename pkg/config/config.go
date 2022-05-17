// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/golang/protobuf/proto"

type Config interface {
	proto.Message
}
