// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

var Kind = primitive.NewKind[setv1.SetServer](register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[setv1.SetServer]) {
	setv1.RegisterSetServer(server, newSetServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[setv1.SetServer], bool) {
	if set, ok := client.(SetProvider); ok {
		return set.GetSet, true
	}
	return nil, false
}

type SetProvider interface {
	GetSet(primitive.ID) setv1.SetServer
}
