// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import "github.com/atomix/runtime/sdk/pkg/logging"

var log = logging.GetLogger()

type Runtime interface {
	GetConn(primitive PrimitiveMeta) (Conn, error)
}
