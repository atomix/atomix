// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/runtime/api/atomix/runtime/topic/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.counter.v1.Topic"

var Type = runtime.NewType[topicv1.TopicServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[topicv1.TopicServer]) {
	topicv1.RegisterTopicServer(server, newTopicServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (topicv1.TopicServer, error) {
	return conn.Topic(config)
}
